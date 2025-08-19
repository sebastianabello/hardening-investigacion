import React, { useEffect, useState } from "react";
import { createSession, eventsUrl, startProcess, downloadZip, ingestES } from "../api";
import { StepIndicator } from "../components/StepIndicator";
import { ChunkedUploader } from "../components/ChunkedUploader";

export default function ProcessPage() {
  const [cliente, setCliente] = useState("");
  const [subcliente, setSubcliente] = useState("");
  const [session, setSession] = useState<string | null>(null);
  const [step, setStep] = useState(1);
  const [log, setLog] = useState<string[]>([]);
  const [indices, setIndices] = useState({ t1n: "qualys_t1_normal", t1a: "qualys_t1_ajustada", t2n: "qualys_t2_normal", t2a: "qualys_t2_ajustada" });

  useEffect(() => {
    if (!session) return;
    const es = new EventSource(eventsUrl(session));
    es.onmessage = (e) => {
      setLog(prev => [...prev, e.data]);
      if (e.data === "status|done") {
        setStep(4);
      }
    };
    return () => es.close();
  }, [session]);

  async function start() {
    const { session_id } = await createSession(cliente || "DEFAULT", subcliente || undefined);
    setSession(session_id);
    setStep(2);
  }

  return (
    <div className="max-w-4xl mx-auto p-6">
      <h1 className="text-2xl font-bold mb-4">Procesar reportes de Qualys</h1>
      <StepIndicator step={step} />

      {step === 1 && (
        <div className="space-y-3">
          <input placeholder="Cliente por defecto" className="border p-2 rounded w-full" value={cliente} onChange={e => setCliente(e.target.value)} />
          <input placeholder="Subcliente (opcional)" className="border p-2 rounded w-full" value={subcliente} onChange={e => setSubcliente(e.target.value)} />
          <button onClick={start} className="px-4 py-2 bg-black text-white rounded">Crear sesión</button>
        </div>
      )}

      {step === 2 && session && (
        <div className="space-y-3">
          <ChunkedUploader sessionId={session} onDone={() => setStep(3)} />
        </div>
      )}

      {step === 3 && session && (
        <div className="space-y-3">
          <button onClick={() => startProcess(session)} className="px-4 py-2 bg-indigo-600 text-white rounded">Iniciar procesamiento</button>
          <div className="p-3 bg-gray-50 rounded h-48 overflow-auto text-sm">
            {log.map((l, i) => (<div key={i}>{l}</div>))}
          </div>
        </div>
      )}

      {step >= 4 && session && (
        <div className="space-y-4">
          <div className="flex gap-2">
            <button onClick={() => downloadZip(session)} className="px-4 py-2 bg-green-600 text-white rounded">Descargar CSVs</button>
          </div>
          <div className="grid grid-cols-2 gap-2">
            <input className="border p-2 rounded" value={indices.t1n} onChange={e=>setIndices({...indices, t1n:e.target.value})} />
            <input className="border p-2 rounded" value={indices.t1a} onChange={e=>setIndices({...indices, t1a:e.target.value})} />
            <input className="border p-2 rounded" value={indices.t2n} onChange={e=>setIndices({...indices, t2n:e.target.value})} />
            <input className="border p-2 rounded" value={indices.t2a} onChange={e=>setIndices({...indices, t2a:e.target.value})} />
          </div>
          <button onClick={async ()=>{ await ingestES(session, indices); }} className="px-4 py-2 bg-amber-600 text-white rounded">Subir a Elasticsearch</button>
          <div className="p-3 bg-gray-50 rounded h-48 overflow-auto text-sm">
            {log.map((l, i) => (<div key={i}>{l}</div>))}
          </div>
        </div>
      )}
    </div>
  );
}
