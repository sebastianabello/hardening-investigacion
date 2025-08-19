from typing import Dict
from threading import Lock
import time

class ProgressBus:
    def __init__(self):
        self._state: Dict[str, Dict] = {}
        self._lock = Lock()

    def init(self, session_id: str):
        with self._lock:
            self._state[session_id] = {"events": [], "status": "created"}

    def push(self, session_id: str, level: str, message: str):
        evt = {"ts": time.time(), "level": level, "message": message}
        with self._lock:
            self._state[session_id]["events"].append(evt)

    def status(self, session_id: str, status: str):
        with self._lock:
            if session_id in self._state:
                self._state[session_id]["status"] = status

    def get_status(self, session_id: str) -> str:
        with self._lock:
            return self._state.get(session_id, {}).get("status", "unknown")

    def stream(self, session_id: str, start_from: int = 0):
        """
        Envía eventos desde start_from (índice del último evento visto + 1).
        Incluye 'id: <n>' para que el cliente pueda reconectar con 'from=<lastId>'.
        """
        last_idx = max(0, int(start_from))
        while True:
            with self._lock:
                evts = self._state.get(session_id, {}).get("events", [])
                status = self._state.get(session_id, {}).get("status", "unknown")

            # Emitir eventos pendientes
            while last_idx < len(evts):
                e = evts[last_idx]
                yield (
                    f"id: {last_idx}\n"
                    f"data: {e['ts']}|{e['level']}|{e['message']}\n\n"
                )
                last_idx += 1

            # Estado final
            if status in ("done", "error"):
                yield f"id: {last_idx}\n" f"data: status|{status}\n\n"
                break

            # Ping preventivo (mantiene viva la conexión)
            yield ": ping\n\n"
            time.sleep(1.0)

bus = ProgressBus()
