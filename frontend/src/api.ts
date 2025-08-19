const API = import.meta.env.VITE_API || "http://localhost:8000";

export async function createSession(cliente: string, subcliente?: string) {
  const res = await fetch(`${API}/sessions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ cliente_por_defecto: cliente, subcliente_por_defecto: subcliente || null })
  });
  if (!res.ok) throw new Error("No se pudo crear la sesión");
  return res.json();
}

export async function initUpload(sessionId: string, filename: string, total: number) {
  const res = await fetch(`${API}/upload/init`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId, filename, total_size: total })
  });
  if (!res.ok) throw new Error("Fallo init upload");
  return res.json();
}

export async function sendChunk(sessionId: string, uploadId: string, filename: string, total: number, start: number, end: number, blob: Blob) {
  const res = await fetch(`${API}/upload/chunk?session_id=${sessionId}&upload_id=${uploadId}&filename=${encodeURIComponent(filename)}&total_size=${total}`, {
    method: "PUT",
    headers: { "Content-Range": `bytes ${start}-${end}/${total}` },
    body: blob
  });
  if (!res.ok) throw new Error("Fallo envío chunk");
  return res.json();
}

export async function completeUpload(sessionId: string, uploadId: string, filename: string) {
  const res = await fetch(`${API}/upload/complete?session_id=${sessionId}&upload_id=${uploadId}&filename=${encodeURIComponent(filename)}`, { method: "POST" });
  if (!res.ok) throw new Error("Fallo complete");
  return res.json();
}

export async function startProcess(sessionId: string) {
  const res = await fetch(`${API}/process`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId })
  });
  if (!res.ok) throw new Error("Fallo iniciar proceso");
  return res.json();
}

export function eventsUrl(sessionId: string, fromId?: string | number) {
  const base = `${API}/sessions/${sessionId}/events`;
  if (fromId !== undefined && fromId !== null && String(fromId).length > 0) {
    return `${base}?from=${encodeURIComponent(String(fromId))}`;
  }
  return base;
}

export async function downloadZip(sessionId: string) {
  const res = await fetch(`${API}/sessions/${sessionId}/results.zip`);
  if (!res.ok) throw new Error("Sin resultados aún");
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = "results.zip"; a.click();
  URL.revokeObjectURL(url);
}

export async function ingestES(sessionId: string, indices: {t1n:string,t1a:string,t2n:string,t2a:string}) {
  const res = await fetch(`${API}/sessions/${sessionId}/ingest`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      session_id: sessionId,
      t1_normal_index: indices.t1n,
      t1_ajustada_index: indices.t1a,
      t2_normal_index: indices.t2n,
      t2_ajustada_index: indices.t2a
    })
  });
  if (!res.ok) throw new Error("Fallo ingesta ES");
  return res.json();
}
