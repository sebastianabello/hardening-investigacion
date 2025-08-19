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

    def stream(self, session_id: str):
        last_idx = 0
        while True:
            with self._lock:
                evts = self._state.get(session_id, {}).get("events", [])
                status = self._state.get(session_id, {}).get("status", "unknown")
            while last_idx < len(evts):
                e = evts[last_idx]
                yield f"data: {e['level']}|{e['message']}\n\n"
                last_idx += 1
            if status in ("done", "error"):
                yield f"data: status|{status}\n\n"
                break
            time.sleep(0.5)

bus = ProgressBus()
