import React from "react";

export function StepIndicator({ step }: { step: number }) {
  const labels = ["Cliente", "Subir", "Procesar", "Validar", "Elasticsearch"];
  return (
    <ol className="flex items-center w-full mb-6">
      {labels.map((label, i) => {
        const idx = i + 1;
        const active = idx <= step;
        return (
          <li key={label} className={"flex-1 flex items-center " + (i < labels.length - 1 ? "after:content-[''] after:flex-1 after:h-0.5 after:bg-gray-300" : "")}>
            <span className={"flex items-center justify-center w-8 h-8 rounded-full mr-2 " + (active ? "bg-green-600 text-white" : "bg-gray-200 text-gray-600")}>{idx}</span>
            <span className={"text-sm " + (active ? "text-green-700" : "text-gray-500")}>{label}</span>
          </li>
        );
      })}
    </ol>
  );
}
