from pydantic import BaseModel, Field
from typing import Optional, List

class SessionCreate(BaseModel):
    cliente_por_defecto: str
    subcliente_por_defecto: Optional[str] = None

class SessionInfo(BaseModel):
    session_id: str
    status: str

class UploadInit(BaseModel):
    session_id: str
    filename: str
    total_size: int

class ProcessRequest(BaseModel):
    session_id: str

class EsIngestRequest(BaseModel):
    session_id: str
    t1_normal_index: str = Field(default="qualys_t1_normal")
    t1_ajustada_index: str = Field(default="qualys_t1_ajustada")
    t2_normal_index: str = Field(default="qualys_t2_normal")
    t2_ajustada_index: str = Field(default="qualys_t2_ajustada")
