import React, { useEffect, useRef, useState } from "react";
import { createSession, startProcess, downloadZip, ingestES } from "../api";
import { StepIndicator } from "../components/StepIndicator";
import { ChunkedUploader } from "../components/ChunkedUploader";

/**
 * Vista principal con SSE robusto:
 * - Cursor (?from=<lastId>) + dedupe por eventId
 * - No reconecta después de status|done / status|error
 * - Candado para evitar doble procesamiento
 */

export default function ProcessPage() {
  // Datos de sesión / cliente
  const [cliente, setCliente] = useState("");
  const [subcliente, setSubcliente] = useState("");

  // Control de sesión y pasos (1=Cliente, 2=Subir, 3=Procesar, 4=Descargar/ES)
  const [session, setSession] = useState<string | null>(null);
  const [step, setStep] = useState<number>(1);

  // Logs y estado
  const [log, setLog] = useState<string[]>([]);
  const [processing, setProcessing] = useState<boolean>(false);

  // Índices de ES
  const [indices, setIndices] = useState({
    t1n: "qualys_t1_normal",
    t1a: "qualys_t1_ajustada",
    t2n: "qualys_t2_normal",
    t2a: "qualys_t2_ajustada",
  });

  // === SSE (una sola conexión), cursor y deduplicación ===
  const esRef = useRef<EventSource | null>(null);
  const lastIdRef = useRef<string | null>(null);
  const seenIdsRef = useRef<Set<string>>(new Set());
  const reconnectTimer = useRef<number | null>(null);
  const finishedRef = useRef<boolean>(false); // evita reconectar tras done/error

  function buildEventsUrl(sessionId: string, fromId?: string | number) {
    const base = `/api/sessions/${sessionId}/events`;
    if (fromId !== undefined && fromId !== null && String(fromId).length > 0) {
      return `${base}?from=${encodeURIComponent(String(fromId))}`;
    }
    return base;
  }

  function closeSSE() {
    if (esRef.current) {
      esRef.current.close();
      esRef.current = null;
    }
    if (reconnectTimer.current) {
      window.clearTimeout(reconnectTimer.current);
      reconnectTimer.current = null;
    }
  }

  function openSSE(sess: string) {
    closeSSE(); // asegura una única conexión

    const url = buildEventsUrl(sess, lastIdRef.current ?? undefined);
    const es = new EventSource(url);
    esRef.current = es;

    es.onmessage = (e: MessageEvent) => {
      const evId = (e as MessageEvent).lastEventId || ""; // viene del backend "id: <n>"
      if (evId) {
        if (seenIdsRef.current.has(evId)) return; // dedupe por ID
        seenIdsRef.current.add(evId);
        lastIdRef.current = evId; // cursor para reconexiones
      }

      const msg = String(e.data ?? "");
      // Evita duplicar status ya finalizado
      if (finishedRef.current && (msg === "status|done" || msg === "status|error")) return;

      setLog((prev) => [...prev, msg]);

      if (msg === "status|done") {
        finishedRef.current = true;
        setProcessing(false);
        setStep(4);
        closeSSE(); // no reconectar más
      } else if (msg === "status|error") {
        finishedRef.current = true;
        setProcessing(false);
        closeSSE();
      }
    };

    es.onerror = () => {
      // Si ya terminó, no reconectar
      if (finishedRef.current) {
        closeSSE();
        return;
      }
      es.close();
      esRef.current = null;
      // Reconexión con backoff corto usando el cursor guardado
      reconnectTimer.current = window.setTimeout(() => openSSE(sess), 1000);
    };
  }

  useEffect(() => {
    if (!session) return;

    // Nueva sesión: reset de cursor/dedupe/estado
    lastIdRef.current = null;
    seenIdsRef.current.clear();
    finishedRef.current = false;
    setLog([]);

    openSSE(session);

    return () => {
      closeSSE();
      seenIdsRef.current.clear();
    };
  }, [session]);

  // === Handlers ===
  async function handleCreateSession() {
    try {
      const { session_id } = await createSession(cliente || "DEFAULT", subcliente || undefined);
      setSession(session_id);
      setStep(2);
      setLog([]);
    } catch {
      setLog((prev) => [...prev, "error|No se pudo crear la sesión"]);
    }
  }

  async function handleStartProcess() {
    if (!session || processing) return;
    try {
      setProcessing(true);
      finishedRef.current = false; // por si re-procesas en una nueva sesión
      await startProcess(session);
      // Progreso vía SSE; al finalizar llega "status|done"
    } catch {
      setProcessing(false);
      setLog((prev) => [...prev, "error|Fallo al iniciar el procesamiento"]);
    }
  }

  async function handleDownload() {
    if (!session) return;
    try {
      await downloadZip(session);
    } catch {
      setLog((prev) => [...prev, "warning|Aún no hay resultados para descargar"]);
    }
  }

  async function handleIngest() {
    if (!session) return;
    try {
      const res = await ingestES(session, indices);
      setLog((prev) => [...prev, `success|Ingesta completada ${JSON.stringify(res.stats)}`]);
    } catch {
      setLog((prev) => [...prev, "error|Fallo al subir a Elasticsearch"]);
    }
  }

  // === Render ===
  return (
    <div className="max-w-4xl mx-auto p-6">
      <h1 className="text-2xl font-bold mb-4">Procesar reportes de Qualys</h1>
      <StepIndicator step={step} />

      {/* Paso 1: Selección de cliente */}
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
          <button onClick={handleCreateSession} className="px-4 py-2 bg-black text-white rounded">
            Crear sesión
          </button>
        </div>
      )}

      {/* Paso 2: Subida de archivos */}
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

      {/* Paso 3: Procesamiento */}
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
            {processing && <span className="text-sm text-gray-600">Procesando… no cierres la página.</span>}
          </div>

          <div className="p-3 bg-gray-50 rounded h-56 overflow-auto text-sm border">
            {log.map((l, i) => (
              <div key={i}>{l}</div>
            ))}
          </div>
        </div>
      )}

      {/* Paso 4: Descarga e Ingesta */}
      {step >= 4 && session && (
        <div className="space-y-4">
          <div className="flex gap-2">
            <button
              onClick={handleDownload}
              className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700"
            >
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
