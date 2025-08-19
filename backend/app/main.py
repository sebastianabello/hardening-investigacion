from fastapi import FastAPI, HTTPException, Request, Response, status, BackgroundTasks, Query

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi import status
import os, uuid, re, shutil, json
from .models import SessionCreate, SessionInfo, UploadInit, ProcessRequest, EsIngestRequest
from .config import settings
from .storage import new_session_dir, session_paths, sanitize_filename, open_chunk_file
from .parser import parse_report_file
from .elastic import bulk_ingest
from .progress import bus
from fastapi import BackgroundTasks

app = FastAPI(title="Qualys CSV Processor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Sesiones ---
@app.post("/sessions", response_model=SessionInfo)
def create_session(payload: SessionCreate):
    sid, sdir = new_session_dir()
    meta = {"cliente_por_defecto": payload.cliente_por_defecto,
            "subcliente_por_defecto": payload.subcliente_por_defecto}
    with open(os.path.join(sdir, "meta.txt"), "w", encoding="utf-8") as f:
        f.write(json.dumps(meta, ensure_ascii=False))
    bus.init(sid)
    bus.push(sid, "info", f"Sesión creada para cliente: {payload.cliente_por_defecto}")
    return SessionInfo(session_id=sid, status="created")

# --- Subida chunked ---
@app.post("/upload/init")
def upload_init(req: UploadInit):
    s = session_paths(req.session_id)
    if not os.path.exists(s["base"]):
        raise HTTPException(status_code=404, detail="Session not found")
    upload_id = uuid.uuid4().hex
    tmp_path = os.path.join(s["uploads"], f"{upload_id}__{sanitize_filename(req.filename)}.part")
    open_chunk_file(tmp_path, req.total_size)
    bus.push(req.session_id, "info", f"Inicio de upload: {req.filename} ({req.total_size} bytes)")
    return {"upload_id": upload_id}

@app.put("/upload/chunk")
async def upload_chunk(request: Request, session_id: str, upload_id: str, filename: str, total_size: int):
    s = session_paths(session_id)
    tmp_path = os.path.join(s["uploads"], f"{upload_id}__{sanitize_filename(filename)}.part")
    if not os.path.exists(tmp_path):
        open_chunk_file(tmp_path, total_size)

    # Content-Range: bytes start-end/total
    cr = request.headers.get("Content-Range")
    if not cr:
        raise HTTPException(411, "Missing Content-Range")
    m = re.match(r"bytes (\d+)-(\d+)/(\d+)", cr)
    if not m: raise HTTPException(400, "Bad Content-Range")
    start, end, total = map(int, m.groups())
    if total != int(total_size): raise HTTPException(400, "total_size mismatch")

    body = await request.body()
    if len(body) != (end - start + 1):
        raise HTTPException(400, "Chunk length mismatch")

    with open(tmp_path, "r+b") as f:
        f.seek(start)
        f.write(body)
    return {"ok": True, "received": len(body)}

@app.post("/upload/complete")
def upload_complete(session_id: str, upload_id: str, filename: str):
    s = session_paths(session_id)
    tmp = os.path.join(s["uploads"], f"{upload_id}__{sanitize_filename(filename)}.part")
    final = os.path.join(s["uploads"], sanitize_filename(filename))
    if not os.path.exists(tmp):
        raise HTTPException(404, "Temp file not found")
    os.replace(tmp, final)
    bus.push(session_id, "info", f"Upload completado: {filename}")
    return {"ok": True, "path": final}

# --- Procesamiento ---
@app.post("/process")
def start_processing(req: ProcessRequest, bg: BackgroundTasks):
    s = session_paths(req.session_id)
    if not os.path.exists(s["base"]):
        raise HTTPException(404, "Session not found")

    # 🔒 evita doble inicio si ya corre
    if bus.get_status(req.session_id) == "running":
        bus.push(req.session_id, "info", "Procesamiento ya en ejecución; ignorado nuevo inicio")
        return {"ok": True, "already_running": True}

    uploads = [os.path.join(s["uploads"], f) for f in os.listdir(s["uploads"]) if not f.endswith(".part")]
    if not uploads:
        raise HTTPException(400, "No hay archivos subidos")

    with open(s["meta"], "r", encoding="utf-8") as f:
        meta = json.loads(f.read() or "{}")
    cliente_default = meta.get("subcliente_por_defecto") or meta.get("cliente_por_defecto") or "DEFAULT"

    def work():
        try:
            bus.status(req.session_id, "running")
            bus.push(req.session_id, "info", f"Comenzando procesamiento de {len(uploads)} archivo(s)")
            for p in uploads:
                bus.push(req.session_id, "info", f"Abriendo {os.path.basename(p)}")
                parse_report_file(p, s["outputs"], cliente_default, req.session_id)
                bus.push(req.session_id, "success", f"Finalizado {os.path.basename(p)}")
            bus.push(req.session_id, "success", "Procesamiento completado")
            bus.status(req.session_id, "done")
        except Exception as e:
            bus.push(req.session_id, "error", f"Fallo en procesamiento: {e}")
            bus.status(req.session_id, "error")

    bg.add_task(work)
    return {"ok": True}

# --- Progreso (SSE) ---
@app.get("/sessions/{session_id}/events")
def stream_events(session_id: str, from_: int | None = Query(default=None, alias="from")):
    # Si el cliente pasa ?from=42, empezamos en 43
    start_from = (from_ + 1) if (from_ is not None and from_ >= 0) else 0
    return StreamingResponse(bus.stream(session_id, start_from=start_from), media_type="text/event-stream")

# --- Descarga de resultados ---
@app.get("/sessions/{session_id}/results.zip")
def download_results(session_id: str):
    import zipfile, tempfile
    s = session_paths(session_id)
    zpath = os.path.join(s["base"], "results.zip")
    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as z:
        outdir = s["outputs"]
        for name in ("t1_normal.csv","t1_ajustada.csv","t2_normal.csv","t2_ajustada.csv"):
            p = os.path.join(outdir, name)
            if os.path.exists(p):
                z.write(p, arcname=name)
    return FileResponse(zpath, filename="results.zip")

# --- Ingesta a Elasticsearch ---
@app.post("/sessions/{session_id}/ingest")
def ingest_es(session_id: str, req: EsIngestRequest):
    s = session_paths(session_id)
    indices = {
        "t1_normal": req.t1_normal_index,
        "t1_ajustada": req.t1_ajustada_index,
        "t2_normal": req.t2_normal_index,
        "t2_ajustada": req.t2_ajustada_index,
    }
    stats = bulk_ingest(session_id, s["outputs"], indices)
    bus.push(session_id, "success", f"Ingesta finalizada: {stats}")
    return {"ok": True, "stats": stats}
