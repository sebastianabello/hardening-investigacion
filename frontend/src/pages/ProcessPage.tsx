import React, { useEffect, useRef, useState } from "react";
import { createSession, eventsUrl, startProcess, downloadZip, ingestES } from "../api";
import { StepIndicator } from "../components/StepIndicator";
import { ChunkedUploader } from "../components/ChunkedUploader";

export default function ProcessPage() {
  const [cliente, setCliente] = useState("");
  const [subcliente, setSubcliente] = useState("");
  const [session, setSession] = useState<string | null>(null);

  // Pasos del wizard:
  // 1 = Cliente, 2 = Subir, 3 = Procesar, 4 = Validar/Descargar/ES
  const [step, setStep] = useState(1);

  const [log, setLog] = useState<string[]>([]);
  const [processing, setProcessing] = useState(false);

  const [indices, setIndices] = useState({
    t1n: "qualys_t1_normal",
    t1a: "qualys_t1_ajustada",
    t2n: "qualys_t2_normal",
    t2a: "qualys_t2_ajustada",
  });

  // === SSE: una sola conexión + deduplicación por event ID ===
  const esRef = useRef<EventSource | null>(null);
  const seenIds = useRef<Set<string>>(new Set());

  useEffect(() => {
    if (!session) return;

    // Cierra previa si existe (defensa)
    if (esRef.current) {
      esRef.current.close();
      esRef.current = null;
    }
    seenIds.current.clear();

    const es = new EventSource(eventsUrl(session));
    esRef.current = es;

    es.onmessage = (e: MessageEvent) => {
      // Dedupe por ID (el backend ahora envía "id: <n>")
      const id = (e as MessageEvent).lastEventId || "";
      if (id) {
        if (seenIds.current.has(id)) return;
        seenIds.current.add(id);
      }
      const msg = String(e.data || "");

      setLog((prev) => [...prev, msg]);

      if (msg === "status|done") {
        setProcessing(false);
        setStep(4);
      } else if (msg === "status|error") {
        setProcessing(false);
      }
    };

    es.onerror = () => {
      // Cierra para evitar múltiples reconexiones en background
      es.close();
      esRef.current = null;
    };

    // Limpieza al desmontar / cambiar de sesión
    return () => {
      es.close();
      esRef.current = null;
      seenIds.current.clear();
    };
  }, [session]);

  async function start() {
    try {
      const { session_id } = await createSession(cliente || "DEFAULT", subcliente || undefined);
      setSession(session_id);
      setStep(2);
      setLog([]);
    } catch (err) {
      console.error(err);
      setLog((prev) => [...prev, "error|No se pudo crear la sesión"]);
    }
  }

  async function handleStartProcess() {
    if (!session || processing) return;
    try {
      setProcessing(true);
      await startProcess(session);
      // El cambio de step a 4 lo hace el SSE cuando reciba "status|done"
    } catch (err) {
      console.error(err);
      setProcessing(false);
      setLog((prev) => [...prev, "error|Fallo al iniciar el procesamiento"]);
    }
  }

  async function handleDownload() {
    if (!session) return;
    try {
      await downloadZip(session);
    } catch (err) {
      console.error(err);
      setLog((prev) => [...prev, "warning|Aún no hay resultados para descargar"]);
    }
  }

  async function handleIngest() {
    if (!session) return;
    try {
      const res = await ingestES(session, indices);
      setLog((prev) => [...prev, `success|Ingesta completada ${JSON.stringify(res.stats)}`]);
    } catch (err) {
      console.error(err);
      setLog((prev) => [...prev, "error|Fallo al subir a Elasticsearch"]);
    }
  }

  return (
    <div className="max-w-4xl mx-auto p-6">
      <h1 className="text-2xl font-bold mb-4">Procesar reportes de Qualys</h1>
      <StepIndicator step={step} />

      {step === 1 && (
        <div className="space-y-3">
          <input
            placeholder="Cliente por defecto"
            className="border p-2 rounded w-full"
            value={cliente}
            onChange={(e) => setCliente(e.target.value)}
          />
          <input
            placeholder="Subcliente (opcional)"
            className="border p-2 rounded w-full"
            value={subcliente}
            onChange={(e) => setSubcliente(e.target.value)}
          />
          <button onClick={start} className="px-4 py-2 bg-black text-white rounded">
            Crear sesión
          </button>
        </div>
      )}

      {step === 2 && session && (
        <div className="space-y-3">
          <ChunkedUploader
            sessionId={session}
            onDone={() => {
              setStep(3);
              setLog((prev) => [...prev, "info|Subida completada"]);
            }}
          />
        </div>
      )}

      {step === 3 && session && (
        <div className="space-y-3">
          <div className="flex items-center gap-2">
            <button
              onClick={handleStartProcess}
              disabled={processing}
              className={`px-4 py-2 rounded text-white ${
                processing ? "bg-indigo-400 cursor-not-allowed" : "bg-indigo-600 hover:bg-indigo-700"
              }`}
            >
              {processing ? "Procesando..." : "Iniciar procesamiento"}
            </button>
            {processing && <span className="text-sm text-gray-600">No cierres esta página hasta finalizar.</span>}
          </div>

          <div className="p-3 bg-gray-50 rounded h-56 overflow-auto text-sm border">
            {log.map((l, i) => (
              <div key={i}>{l}</div>
            ))}
          </div>
        </div>
      )}

      {step >= 4 && session && (
        <div className="space-y-4">
          <div className="flex gap-2">
            <button onClick={handleDownload} className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
              Descargar CSVs
            </button>
          </div>

          <div className="grid grid-cols-2 gap-2">
            <input
              className="border p-2 rounded"
              value={indices.t1n}
              onChange={(e) => setIndices({ ...indices, t1n: e.target.value })}
              placeholder="Índice T1 Normal"
            />
            <input
              className="border p-2 rounded"
              value={indices.t1a}
              onChange={(e) => setIndices({ ...indices, t1a: e.target.value })}
              placeholder="Índice T1 Ajustada"
            />
            <input
              className="border p-2 rounded"
              value={indices.t2n}
              onChange={(e) => setIndices({ ...indices, t2n: e.target.value })}
              placeholder="Índice T2 Normal"
            />
            <input
              className="border p-2 rounded"
              value={indices.t2a}
              onChange={(e) => setIndices({ ...indices, t2a: e.target.value })}
              placeholder="Índice T2 Ajustada"
            />
          </div>

          <button
            onClick={handleIngest}
            className="px-4 py-2 bg-amber-600 text-white rounded hover:bg-amber-700"
          >
            Subir a Elasticsearch
          </button>

          <div className="p-3 bg-gray-50 rounded h-56 overflow-auto text-sm border">
            {log.map((l, i) => (
              <div key={i}>{l}</div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
