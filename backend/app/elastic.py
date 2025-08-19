import csv, os, json, requests
from typing import Dict, Iterator, Optional
from .config import settings
from .progress import bus

def _auth():
    # Usa Basic solo si NO hay API key
    if settings.ES_API_KEY:
        return None
    if settings.ES_USERNAME:
        return (settings.ES_USERNAME, settings.ES_PASSWORD)
    return None

def _headers():
    h = {"Content-Type": "application/x-ndjson"}
    if settings.ES_API_KEY:
        # Authorization: ApiKey <base64(id:key)>   (o la “Encoded API key” de Kibana)
        h["Authorization"] = f"ApiKey {settings.ES_API_KEY}"
    return h

def _verify_opt() -> Optional[object]:
    # requests.verify puede ser bool o ruta a CA
    if settings.ES_CA_CERT:
        return settings.ES_CA_CERT
    return settings.ES_VERIFY_SSL

def _actions_from_csv(path: str, index: str) -> Iterator[str]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            meta = {"index": {"_index": index}}
            yield json.dumps(meta) + "\n"
            yield json.dumps(row, ensure_ascii=False) + "\n"

def bulk_ingest(session_id: str, outputs_dir: str, indices: Dict[str, str]) -> Dict[str, int]:
    files = {
        "t1_normal":   os.path.join(outputs_dir, "t1_normal.csv"),
        "t1_ajustada": os.path.join(outputs_dir, "t1_ajustada.csv"),
        "t2_normal":   os.path.join(outputs_dir, "t2_normal.csv"),
        "t2_ajustada": os.path.join(outputs_dir, "t2_ajustada.csv"),
    }
    stats: Dict[str, int] = {}
    url = settings.ES_BASE_URL.rstrip("/") + "/_bulk"
    headers = _headers()
    auth = _auth()
    verify = _verify_opt()

    for key, path in files.items():
        if not os.path.exists(path):
            stats[key] = 0
            continue

        bus.push(session_id, "info", f"Ingestando {key} → {indices[key]}")
        sent = 0
        with requests.Session() as s:
            def gen():
                nonlocal sent
                for line in _actions_from_csv(path, indices[key]):
                    sent += 1
                    yield line.encode("utf-8")
            resp = s.post(url, data=gen(), headers=headers, auth=auth, stream=True, verify=verify)
            resp.raise_for_status()
            rj = resp.json()
            if rj.get("errors"):
                bus.push(session_id, "warning", f"ES devolvió errors=true para {key}")
        stats[key] = sent // 2  # cada doc son 2 líneas NDJSON (_bulk)
    return stats
