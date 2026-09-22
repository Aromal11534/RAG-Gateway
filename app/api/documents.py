import asyncio
import json
import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Path, Query, Response, status
from pydantic import BaseModel, Field, field_validator, model_validator

from app.api.vectors import VectorItem, _write
from app.config import settings
from app.consistency.revision import newest
from app.database.oracle import delete_document, get_document_chunks
from app.embeddings.adapter import embed_texts
from app.ingestion.chunker import chunk_text
from app.jobs.manager import job_manager
from app.router.shard_registry import registry

router = APIRouter(prefix="/documents", tags=["documents"])


class DocumentIngestRequest(BaseModel):
    id: Optional[str] = Field(default=None, min_length=1, max_length=480)
    namespace: str = Field(min_length=1, max_length=128)
    text: str = Field(min_length=1)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    chunk_size: Optional[int] = Field(default=None, ge=100, le=100_000)
    chunk_overlap: Optional[int] = Field(default=None, ge=0)
    replace_existing: bool = True

    @field_validator("id", "namespace", "text")
    @classmethod
    def reject_blank_strings(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value

    @model_validator(mode="after")
    def validate_document(self):
        if len(self.text) > settings.max_text_length:
            raise ValueError("text is too long")
        size = self.chunk_size or settings.default_chunk_size
        overlap = (
            self.chunk_overlap if self.chunk_overlap is not None else settings.default_chunk_overlap
        )
        if overlap >= size:
            raise ValueError("chunk_overlap must be smaller than chunk_size")
        encoded_metadata = json.dumps(self.metadata, ensure_ascii=False).encode("utf-8")
        if len(encoded_metadata) > settings.max_metadata_bytes:
            raise ValueError("metadata is too large")
        return self


async def _delete_everywhere(document_id: str, namespace: str) -> int:
    candidates = [shard_id for shard_id in registry.shards if registry.reserve(shard_id)]
    if len(candidates) != len(registry.shards):
        raise HTTPException(
            status_code=503,
            detail="Document replacement requires every shard to be available",
        )

    async def delete_one(shard_id: str) -> tuple[int, bool]:
        try:
            affected = await asyncio.wait_for(
                delete_document(shard_id, document_id, namespace),
                timeout=settings.shard_query_timeout_seconds,
            )
            registry.circuit_breaker.record_success(shard_id)
            return affected, True
        except Exception:
            registry.circuit_breaker.record_failure(shard_id)
            return 0, False

    outcomes = await asyncio.gather(*(delete_one(shard_id) for shard_id in candidates))
    if not all(succeeded for _, succeeded in outcomes):
        raise HTTPException(
            status_code=503,
            detail="Document deletion was partially completed; retry is safe",
        )
    return sum(count for count, _ in outcomes)


async def _ingest_document(req: DocumentIngestRequest) -> dict:
    document_id = req.id or str(uuid.uuid4())
    size = req.chunk_size or settings.default_chunk_size
    overlap = req.chunk_overlap if req.chunk_overlap is not None else settings.default_chunk_overlap
    chunks = chunk_text(req.text, size=size, overlap=overlap)
    if len(chunks) > settings.max_document_chunks:
        raise HTTPException(
            status_code=422,
            detail=f"Document produced more than {settings.max_document_chunks} chunks",
        )
    try:
        embeddings = await embed_texts([chunk.text for chunk in chunks])
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Embedding service is unavailable",
        ) from exc
    if req.replace_existing:
        await _delete_everywhere(document_id, req.namespace)

    semaphore = asyncio.Semaphore(settings.max_batch_concurrency)

    async def ingest_one(index: int) -> dict:
        chunk = chunks[index]
        metadata = {
            **req.metadata,
            "_rag_gateway": {
                "document_id": document_id,
                "chunk_index": index,
                "chunk_count": len(chunks),
                "start_char": chunk.start_char,
                "end_char": chunk.end_char,
            },
        }
        item = VectorItem(
            id=f"{document_id}:chunk:{index:06d}",
            namespace=req.namespace,
            text=chunk.text,
            metadata=metadata,
            document_id=document_id,
            chunk_index=index,
        )
        async with semaphore:
            return await _write(item, upsert=True, embedding=embeddings[index])

    results = await asyncio.gather(
        *(ingest_one(index) for index in range(len(chunks))),
        return_exceptions=True,
    )
    failures = [str(result) for result in results if isinstance(result, Exception)]
    if failures:
        raise RuntimeError(
            f"{len(failures)} of {len(chunks)} chunks failed; retry the document ingestion"
        )
    return {
        "status": "ingested",
        "id": document_id,
        "namespace": req.namespace,
        "chunks": len(chunks),
        "partial_chunks": sum(bool(result.get("partial")) for result in results),
    }


@router.post("")
async def ingest_document(
    req: DocumentIngestRequest,
    response: Response,
    background: bool = Query(default=False),
):
    if background:
        operation = job_manager.submit("document_ingestion", _ingest_document(req))
        response.status_code = status.HTTP_202_ACCEPTED
        return operation
    return await _ingest_document(req)


@router.get("/{document_id}")
async def get_document(
    document_id: str = Path(min_length=1, max_length=480),
    namespace: str = Query(min_length=1, max_length=128),
):
    candidates = [shard_id for shard_id in registry.shards if registry.reserve(shard_id)]
    if not candidates:
        raise HTTPException(status_code=503, detail="No database shard is available")

    async def get_one(shard_id: str) -> tuple[list[dict], bool]:
        try:
            chunks = await asyncio.wait_for(
                get_document_chunks(shard_id, namespace, document_id),
                timeout=settings.shard_query_timeout_seconds,
            )
            registry.circuit_breaker.record_success(shard_id)
            return chunks, True
        except Exception:
            registry.circuit_breaker.record_failure(shard_id)
            return [], False

    outcomes = await asyncio.gather(*(get_one(shard_id) for shard_id in candidates))
    latest_by_id: dict[str, dict] = {}
    for chunks, _ in outcomes:
        for chunk in chunks:
            latest_by_id[chunk["id"]] = newest(
                [latest_by_id[chunk["id"]], chunk] if chunk["id"] in latest_by_id else [chunk]
            )
    if not latest_by_id:
        completed = sum(1 for _, succeeded in outcomes if succeeded)
        if completed != len(registry.shards):
            raise HTTPException(status_code=503, detail="Document lookup was incomplete")
        raise HTTPException(status_code=404, detail="Document not found")
    return {
        "id": document_id,
        "namespace": namespace,
        "chunks": sorted(
            latest_by_id.values(),
            key=lambda item: (item.get("chunk_index") or 0, item["id"]),
        ),
        "partial": sum(1 for _, succeeded in outcomes if succeeded) != len(registry.shards),
    }


@router.delete("/{document_id}")
async def delete_document_endpoint(
    document_id: str = Path(min_length=1, max_length=480),
    namespace: str = Query(min_length=1, max_length=128),
):
    affected = await _delete_everywhere(document_id, namespace)
    if affected == 0:
        raise HTTPException(status_code=404, detail="Document not found")
    return {
        "status": "deleted",
        "id": document_id,
        "namespace": namespace,
        "deleted_chunks": affected,
    }
