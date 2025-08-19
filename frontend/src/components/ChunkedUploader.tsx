import React, { useState } from "react";
import { initUpload, sendChunk, completeUpload } from "../api";

const CHUNK = 8 * 1024 * 1024; // 8MB

export function ChunkedUploader({ sessionId, onDone }: { sessionId: string; onDone: () => void }) {
  const [progress, setProgress] = useState<Record<string, number>>({});

  async function uploadFile(file: File) {
    const { upload_id } = await initUpload(sessionId, file.name, file.size);
    let offset = 0;
    while (offset < file.size) {
      const end = Math.min(offset + CHUNK, file.size);
      const blob = file.slice(offset, end);
      await sendChunk(sessionId, upload_id, file.name, file.size, offset, end - 1, blob);
      offset = end;
      setProgress(p => ({ ...p, [file.name]: Math.round((offset / file.size) * 100) }));
    }
    await completeUpload(sessionId, upload_id, file.name);
  }

  async function handleFiles(files: FileList | null) {
    if (!files || !files.length) return;
    for (const f of Array.from(files)) {
      await uploadFile(f);
    }
    onDone();
  }

  return (
    <div className="p-4 border-2 border-dashed rounded-2xl">
      <input type="file" multiple onChange={e => handleFiles(e.target.files)} className="mb-2" />
      {Object.entries(progress).map(([name, pct]) => (
        <div key={name} className="mb-2">
          <div className="text-sm">{name} — {pct}%</div>
          <div className="w-full bg-gray-200 rounded h-2"><div className="h-2 rounded bg-green-600" style={{ width: `${pct}%` }} /></div>
        </div>
      ))}
    </div>
  );
}
