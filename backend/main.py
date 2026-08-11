import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# On Windows the console/pipe encoding defaults to the system codepage (e.g. cp1252),
# which can't encode the Cyrillic text this app prints/logs. Force UTF-8 so print()
# calls in backend/db.py, weaviate_funcs.py etc. don't crash the process.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8")

from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend.db import (
    delete_document_postgres,
    ensure_action_logs_table,
    ensure_marketing_files_table,
    get_action_logs,
    init_pg_pool,
    log_action,
    search_marketing_files_pg,
)
from backend.weaviate_funcs import (
    SUPPORTED_EXTENSIONS,
    add_text_document_to_weaviate,
    delete_document_weaviate,
    ensure_schema,
    get_chunks_weaviate,
    get_client,
)
from const import COLLECTION_NAME

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_pg_pool()
    ensure_marketing_files_table()
    ensure_action_logs_table()
    client = get_client()
    ensure_schema(client, COLLECTION_NAME)
    yield


app = FastAPI(title="Альфа Ойл — База знаний API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class DocumentOut(BaseModel):
    id_doc: str
    name: str
    added_date: str
    chunks_count: int


class ChunksOut(BaseModel):
    id_doc: str
    chunks: List[str]


class UploadResult(BaseModel):
    name: str
    ok: bool
    message: str


class LogEntry(BaseModel):
    id: int
    action: str
    status: str
    doc_id: Optional[str] = None
    doc_name: Optional[str] = None
    message: Optional[str] = None
    created_at: Optional[str] = None


class _InMemoryFile:
    """Adapter exposing the .name / .read() interface that
    add_text_document_to_weaviate expects, built from FastAPI's UploadFile."""

    def __init__(self, name: str, data: bytes):
        self.name = name
        self._data = data

    def read(self) -> bytes:
        return self._data


@app.get("/api/documents", response_model=List[DocumentOut])
def list_documents(search: Optional[str] = None):
    docs = search_marketing_files_pg(title=search)
    return [
        DocumentOut(
            id_doc=d["id_doc"],
            name=d["name"],
            added_date=str(d["added_date"]),
            chunks_count=d["chunks_count"],
        )
        for d in docs
    ]


@app.get("/api/documents/{id_doc}/chunks", response_model=ChunksOut)
def get_document_chunks(id_doc: str):
    client = get_client()
    try:
        chunks = get_chunks_weaviate(client, id_doc)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return ChunksOut(id_doc=id_doc, chunks=chunks)


@app.delete("/api/documents/{id_doc}")
def delete_document(id_doc: str, name: Optional[str] = None):
    client = get_client()
    try:
        delete_document_postgres(id_doc)
        delete_document_weaviate(client, id_doc)
    except ValueError as e:
        log_action("delete", "error", doc_id=id_doc, doc_name=name, message=str(e))
        raise HTTPException(status_code=400, detail=str(e)) from e
    log_action("delete", "success", doc_id=id_doc, doc_name=name)
    return {"ok": True}


@app.post("/api/documents/upload", response_model=List[UploadResult])
async def upload_documents(files: List[UploadFile] = File(...)):
    results: List[UploadResult] = []
    for file in files:
        filename = file.filename or ""
        if not filename.lower().endswith(SUPPORTED_EXTENSIONS):
            message = f"Поддерживаются только файлы: {', '.join(SUPPORTED_EXTENSIONS)}"
            log_action("upload", "error", doc_name=filename, message=message)
            results.append(UploadResult(name=filename, ok=False, message=message))
            continue
        data = await file.read()
        try:
            doc_id = add_text_document_to_weaviate(_InMemoryFile(filename, data))
            log_action("upload", "success", doc_id=doc_id, doc_name=filename, message="Документ добавлен в базу")
            results.append(UploadResult(name=filename, ok=True, message="Документ добавлен в базу"))
        except ValueError as e:
            log_action("upload", "error", doc_name=filename, message=str(e))
            results.append(UploadResult(name=filename, ok=False, message=str(e)))
    return results


@app.get("/api/logs", response_model=List[LogEntry])
def list_logs(limit: int = 200):
    return get_action_logs(limit=limit)


@app.get("/api/health")
def health():
    return {"status": "ok"}


# --- Static frontend (vanilla JS/HTML/CSS, no build step) ---
app.mount("/assets", StaticFiles(directory=FRONTEND_DIR), name="assets")


@app.get("/")
def index():
    return FileResponse(FRONTEND_DIR / "index.html")
