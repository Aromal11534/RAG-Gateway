import asyncio
import json
import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Path, Query, Response, status
from pydantic import BaseModel, Field, field_validator, model_validator

from app.api.vectors import (
    VectorItem,
    _all_placements,
    _delete_vector_everywhere,
    _get_one,
    _reserve_placements,
    _write,
)
from app.config import settings
from app.consistency.revision import newest, revision_generator
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


async def _get_manifest(document_id: str, namespace: str) -> str | None:
    manifest_id = f"doc_manifest:{document_id}"
    placements = _all_placements(manifest_id, namespace)
    candidates = _reserve_placements(placements)
    outcomes = await asyncio.gather(
        *(_get_one(shard_id, manifest_id, namespace) for shard_id in candidates)
    )
    found = [res for res, succeeded in outcomes if succeeded and res is not None]
    latest = newest(found)
    if latest and not latest.get("is_deleted"):
        return latest.get("metadata", {}).get("active_generation")
    return None


async def _delete_everywhere(document_id: str, namespace: str, revision: int) -> int:
    candidates = [shard_id for shard_id in registry.shards if registry.reserve(shard_id)]
    if len(candidates) != len(registry.shards):
        raise HTTPException(
            status_code=503,
            detail="Document replacement requires every shard to be available",
        )

    async def delete_one(shard_id: str) -> tuple[int, bool]:
        try:
            affected = await asyncio.wait_for(
                delete_document(shard_id, document_id, namespace, revision),
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

    old_generation_id = None
    if req.replace_existing:
        old_generation_id = await _get_manifest(document_id, req.namespace)

    generation_id = str(uuid.uuid4())
    semaphore = asyncio.Semaphore(settings.max_batch_concurrency)

    async def ingest_one(index: int) -> dict:
        chunk = chunks[index]
        metadata = {
            **req.metadata,
            "_rag_gateway": {
                "document_id": document_id,
                "generation_id": generation_id,
                "chunk_index": index,
                "chunk_count": len(chunks),
                "start_char": chunk.start_char,
                "end_char": chunk.end_char,
            },
        }
        item = VectorItem(
            id=f"{generation_id}:chunk:{index:06d}",
            namespace=req.namespace,
            text=chunk.text,
            metadata=metadata,
            document_id=generation_id,
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

    manifest_item = VectorItem(
        id=f"doc_manifest:{document_id}",
        namespace=req.namespace,
        text=f"Manifest for {document_id}",
        metadata={"active_generation": generation_id},
        document_id=document_id,
        chunk_index=0,
    )
    await _write(manifest_item, upsert=True)

    if old_generation_id:
        revision = revision_generator.next()
        job_manager.submit(
            "cleanup_old_generation",
            {
                "document_id": old_generation_id,
                "namespace": req.namespace,
                "revision": revision,
            },
        )

    return {
        "status": "ingested",
        "id": document_id,
        "namespace": req.namespace,
        "chunks": len(chunks),
        "partial_chunks": sum(
            bool(result.get("partial")) for result in results if isinstance(result, dict)
        ),
    }


@router.post("")
async def ingest_document(
    req: DocumentIngestRequest,
    response: Response,
    background: bool = Query(default=False),
):
    if background:
        operation = job_manager.submit("document_ingestion", req.model_dump())
        response.status_code = status.HTTP_202_ACCEPTED
        return operation
    return await _ingest_document(req)


# Register job handlers
job_manager.register(
    "document_ingestion",
    lambda p: _ingest_document(DocumentIngestRequest(**p)),
)
job_manager.register(
    "cleanup_old_generation",
    lambda p: _delete_everywhere(p["document_id"], p["namespace"], p["revision"]),
)


@router.get("/{document_id}")
async def get_document(
    document_id: str = Path(min_length=1, max_length=480),
    namespace: str = Query(min_length=1, max_length=128),
):
    active_generation = await _get_manifest(document_id, namespace)
    if not active_generation:
        raise HTTPException(status_code=404, detail="Document not found")

    candidates = [shard_id for shard_id in registry.shards if registry.reserve(shard_id)]
    if not candidates:
        raise HTTPException(status_code=503, detail="No database shard is available")

    async def get_one(shard_id: str) -> tuple[list[dict], bool]:
        try:
            chunks = await asyncio.wait_for(
                get_document_chunks(shard_id, namespace, active_generation),
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
        raise HTTPException(status_code=404, detail="Document chunks missing")
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
    active_generation = await _get_manifest(document_id, namespace)
    if not active_generation:
        raise HTTPException(status_code=404, detail="Document not found")

    revision = revision_generator.next()
    await _delete_vector_everywhere(f"doc_manifest:{document_id}", namespace, revision)
    affected = await _delete_everywhere(active_generation, namespace, revision)

    return {
        "status": "deleted",
        "id": document_id,
        "namespace": namespace,
        "deleted_chunks": affected,
    }
