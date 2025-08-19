import os, uuid, re
from typing import Tuple
from .config import settings

os.makedirs(settings.DATA_DIR, exist_ok=True)

def new_session_dir() -> Tuple[str, str]:
    sid = uuid.uuid4().hex
    sdir = os.path.join(settings.DATA_DIR, sid)
    os.makedirs(os.path.join(sdir, "uploads"), exist_ok=True)
    os.makedirs(os.path.join(sdir, "outputs"), exist_ok=True)
    return sid, sdir

def session_paths(session_id: str):
    base = os.path.join(settings.DATA_DIR, session_id)
    return {
        "base": base,
        "uploads": os.path.join(base, "uploads"),
        "outputs": os.path.join(base, "outputs"),
        "meta": os.path.join(base, "meta.txt"),
    }

def sanitize_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)

def open_chunk_file(tmp_path: str, total_size: int):
    """Pre-create file if not exists to allow random-access writes."""
    if not os.path.exists(tmp_path):
        with open(tmp_path, "wb") as f:
            f.truncate(total_size)
