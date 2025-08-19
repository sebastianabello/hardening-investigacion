import os
from dotenv import load_dotenv
load_dotenv()

def _to_bool(x: str, default: bool = True) -> bool:
    if x is None:
        return default
    return str(x).strip().lower() in ("1","true","yes","y")

class Settings:
    ES_BASE_URL: str = os.getenv("ES_BASE_URL", "http://localhost:9200")
    ES_USERNAME: str = os.getenv("ES_USERNAME", "")      # fallback (básico), opcional
    ES_PASSWORD: str = os.getenv("ES_PASSWORD", "")      # fallback (básico), opcional
    ES_API_KEY: str = os.getenv("ES_API_KEY", "")        # <<— usa ApiKey si viene
    ES_VERIFY_SSL: bool = _to_bool(os.getenv("ES_VERIFY_SSL", "true"), True)
    ES_CA_CERT: str = os.getenv("ES_CA_CERT", "")        # path a CA bundle opcional

    DATA_DIR: str = os.getenv("DATA_DIR", "/tmp/app/sessions")
    CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", "8388608"))  # 8MB
    MAX_CONCURRENCY: int = int(os.getenv("MAX_CONCURRENCY", "2"))

settings = Settings()
