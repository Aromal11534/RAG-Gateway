import asyncio
import json
import logging
import uuid
from typing import Any, Dict, Optional

import oracledb
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Header,
    HTTPException,
    Path,
    Query,
    Response,
    status,
)
from pydantic import BaseModel, Field, field_validator, model_validator

from app.config import settings
from app.consistency.reconciler import repair_vector_replicas
from app.consistency.revision import content_hash, newest, revision_generator
from app.database.oracle import delete_vector as delete_vector_from_shard
from app.database.oracle import get_vector as get_vector_from_shard
from app.database.oracle import insert_vector, upsert_vector, StaleRevisionError
from app.embeddings.adapter import embed_text, embed_texts
from app.router.shard_registry import registry

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/vectors", tags=["vectors"])


class VectorItem(BaseModel):
    id: Optional[str] = Field(default=None, min_length=1, max_length=512)
    namespace: str = Field(min_length=1, max_length=128)
    text: str = Field(min_length=1)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    document_id: Optional[str] = Field(default=None, min_length=1, max_length=512)
    chunk_index: Optional[int] = Field(default=None, ge=0)

    @field_validator("id", "namespace", "text", "document_id")
    @classmethod
    def reject_blank_strings(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value

    @model_validator(mode="after")
    def enforce_payload_limits(self):
        if len(self.text) > settings.max_text_length:
            raise ValueError("text is too long")
        metadata_size = len(
            json.dumps(self.metadata, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        if metadata_size > settings.max_metadata_bytes:
            raise ValueError("metadata is too large")
        return self


class BatchVectorRequest(BaseModel):
    items: list[VectorItem] = Field(
        min_length=1,
        max_length=settings.max_batch_size,
    )
    upsert: bool = True


async def _embedding_for(text: str) -> list[float]:
    try:
        return await embed_text(text)
    except Exception:
        logger.exception("Vector embedding failed")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Embedding service is unavailable",
        ) from None


def _all_placements(item_id: str, namespace: str) -> list[str]:
    return list(registry.hash_ring.get_nodes(f"{namespace}:{item_id}"))


def _reserve_placements(
    placements: list[str],
    *,
    limit: int | None = None,
    exclude: set[str] | None = None,
) -> list[str]:
    reserved: list[str] = []
    excluded = exclude or set()
    for shard_id in placements:
        if shard_id in excluded:
            continue
        if registry.reserve(shard_id):
            reserved.append(shard_id)
            if limit is not None and len(reserved) >= limit:
                break
    return reserved


async def _write_one(
    shard_id: str,
    item: VectorItem,
    item_id: str,
    embedding: list[float],
    revision: int,
    digest: str,
    *,
    upsert: bool,
) -> tuple[bool, bool]:
    operation = upsert_vector if upsert else insert_vector
    try:
        await asyncio.wait_for(
            operation(
                shard_id,
                item_id,
                item.namespace,
                item.text,
                item.metadata,
                embedding,
                revision=revision,
                content_hash=digest,
                document_id=item.document_id,
                chunk_index=item.chunk_index,
            ),
            timeout=settings.shard_query_timeout_seconds,
        )
        registry.circuit_breaker.record_success(shard_id)
        return True, False
    except (oracledb.IntegrityError, StaleRevisionError):
        return False, True
    except Exception:
        registry.circuit_breaker.record_failure(shard_id)
        logger.exception("Vector write failed on shard %s", shard_id)
        return False, False


async def _get_one(shard_id: str, item_id: str, namespace: str):
    try:
        result = await asyncio.wait_for(
            get_vector_from_shard(shard_id, item_id, namespace),
            timeout=settings.shard_query_timeout_seconds,
        )
        registry.circuit_breaker.record_success(shard_id)
        return result, True
    except Exception:
        registry.circuit_breaker.record_failure(shard_id)
        logger.exception("Vector read failed on shard %s", shard_id)
        return None, False


async def _ensure_absent(item_id: str, namespace: str, placements: list[str]) -> None:
    candidates = _reserve_placements(placements)
    if len(candidates) != len(registry.shards):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Insert uniqueness requires every shard to be available",
        )
    outcomes = await asyncio.gather(
        *(_get_one(shard_id, item_id, namespace) for shard_id in candidates)
    )
    if not all(succeeded for _, succeeded in outcomes):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Insert uniqueness could not be checked on every shard",
        )
    if any(result is not None for result, _ in outcomes):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A vector with this ID already exists in the namespace",
        )


async def _write(
    item: VectorItem,
    *,
    upsert: bool,
    embedding: list[float] | None = None,
) -> dict:
    item_id = item.id or str(uuid.uuid4())
    placements = _all_placements(item_id, item.namespace)
    if not placements:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="No database shard is configured",
        )

    if not upsert:
        await _ensure_absent(item_id, item.namespace, placements)

    desired_replicas = min(settings.replication_factor, len(placements))
    quorum = min(settings.write_quorum, desired_replicas)
    targets = _reserve_placements(placements, limit=desired_replicas)
    if len(targets) < quorum:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Not enough database shards are available to satisfy the write quorum",
        )

    embedding = embedding if embedding is not None else await _embedding_for(item.text)
    revision = revision_generator.next()
    digest = content_hash(item_id, item.namespace, item.text, item.metadata)
    outcomes = await asyncio.gather(
        *(
            _write_one(
                shard_id,
                item,
                item_id,
                embedding,
                revision,
                digest,
                upsert=upsert,
            )
            for shard_id in targets
        )
    )

    successful_shards = [
        shard_id for shard_id, (succeeded, _) in zip(targets, outcomes, strict=True) if succeeded
    ]
    conflict = any(is_conflict for _, is_conflict in outcomes)

    if len(successful_shards) < quorum:
        fallback_targets = _reserve_placements(
            placements,
            limit=quorum - len(successful_shards),
            exclude=set(targets),
        )
        for shard_id in fallback_targets:
            succeeded, is_conflict = await _write_one(
                shard_id,
                item,
                item_id,
                embedding,
                revision,
                digest,
                upsert=upsert,
            )
            conflict = conflict or is_conflict
            if succeeded:
                successful_shards.append(shard_id)

    if conflict and not upsert:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A vector with this ID already exists in the namespace",
        )
    if len(successful_shards) < quorum:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database write did not satisfy the configured quorum",
        )

    return {
        "status": "upserted" if upsert else "inserted",
        "id": item_id,
        "namespace": item.namespace,
        "revision": revision,
        "content_hash": digest,
        "replicas_written": successful_shards,
        "partial": len(successful_shards) < desired_replicas,
    }


async def _delete_one(shard_id: str, item_id: str, namespace: str, revision: int):
    try:
        affected = await asyncio.wait_for(
            delete_vector_from_shard(shard_id, item_id, namespace, revision),
            timeout=settings.shard_query_timeout_seconds,
        )
        registry.circuit_breaker.record_success(shard_id)
        return affected, True
    except Exception:
        registry.circuit_breaker.record_failure(shard_id)
        logger.exception("Vector deletion failed on shard %s", shard_id)
        return 0, False


def _clean_namespace(namespace: str) -> str:
    namespace = namespace.strip()
    if not namespace:
        raise HTTPException(status_code=422, detail="namespace must not be blank")
    return namespace


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED, include_in_schema=False)
async def insert_vector_endpoint(item: VectorItem):
    return await _write(item, upsert=False)


@router.post("/batch")
async def batch_vectors_endpoint(req: BatchVectorRequest, response: Response):
    semaphore = asyncio.Semaphore(settings.max_batch_concurrency)
    try:
        embeddings = await embed_texts([item.text for item in req.items])
    except Exception:
        logger.exception("Batch embedding failed")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Embedding service is unavailable",
        ) from None

    async def run_one(index: int, item: VectorItem) -> dict:
        async with semaphore:
            try:
                result = await _write(
                    item,
                    upsert=req.upsert,
                    embedding=embeddings[index],
                )
                return {"index": index, "ok": True, **result}
            except HTTPException as exc:
                return {
                    "index": index,
                    "ok": False,
                    "status_code": exc.status_code,
                    "detail": exc.detail,
                }

    results = await asyncio.gather(*(run_one(index, item) for index, item in enumerate(req.items)))
    succeeded = sum(1 for result in results if result["ok"])
    if succeeded != len(results):
        response.status_code = status.HTTP_207_MULTI_STATUS
    return {
        "items": results,
        "succeeded": succeeded,
        "failed": len(results) - succeeded,
    }


@router.put("/{item_id}")
async def upsert_vector_endpoint(
    item: VectorItem,
    item_id: str = Path(min_length=1, max_length=512),
):
    item.id = item_id.strip()
    if not item.id:
        raise HTTPException(status_code=422, detail="id must not be blank")
    return await _write(item, upsert=True)


@router.get("/{item_id}")
async def get_vector_endpoint(
    background_tasks: BackgroundTasks,
    response: Response,
    item_id: str = Path(min_length=1, max_length=512),
    namespace: str = Query(min_length=1, max_length=128),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
):
    namespace = _clean_namespace(namespace)
    placements = _all_placements(item_id, namespace)
    candidates = _reserve_placements(placements)
    if not candidates:
        raise HTTPException(status_code=503, detail="No database shard is available")

    outcomes = await asyncio.gather(
        *(_get_one(shard_id, item_id, namespace) for shard_id in candidates)
    )
    observed = {
        shard_id: result
        for shard_id, (result, succeeded) in zip(candidates, outcomes, strict=True)
        if succeeded
    }
    found = [result for result, _ in outcomes if result is not None]
    latest = newest(found)
    completed = sum(1 for _, succeeded in outcomes if succeeded)

    if latest is not None:
        intended = placements[: min(settings.replication_factor, len(placements))]
        background_tasks.add_task(
            repair_vector_replicas,
            latest,
            intended,
            observed,
        )
        if latest.get("is_deleted"):
            raise HTTPException(status_code=404, detail="Vector not found")
        digest = latest.get("content_hash")
        if digest:
            etag = f'"{digest}"'
            response.headers["ETag"] = etag
            if if_none_match == etag:
                response.status_code = status.HTTP_304_NOT_MODIFIED
                return None
        payload = {key: value for key, value in latest.items() if not key.startswith("_")}
        payload["replicas_checked"] = completed
        payload["partial"] = completed != len(registry.shards)
        return payload

    if completed != len(registry.shards):
        raise HTTPException(
            status_code=503,
            detail="The vector could not be checked on every shard",
        )
    raise HTTPException(status_code=404, detail="Vector not found")


async def _delete_vector_everywhere(item_id: str, namespace: str, revision: int) -> int:
    placements = _all_placements(item_id, namespace)
    candidates = _reserve_placements(placements)
    if len(candidates) != len(registry.shards):
        raise HTTPException(
            status_code=503,
            detail="Deletion requires every shard to be available",
        )
    outcomes = await asyncio.gather(
        *(_delete_one(shard_id, item_id, namespace, revision) for shard_id in candidates)
    )
    affected = sum(count for count, _ in outcomes)
    if not all(succeeded for _, succeeded in outcomes):
        raise HTTPException(
            status_code=503,
            detail="Deletion was only partially completed; retry is safe",
        )
    return affected

@router.delete("/{item_id}")
async def delete_vector_endpoint(
    item_id: str = Path(min_length=1, max_length=512),
    namespace: str = Query(min_length=1, max_length=128),
):
    namespace = _clean_namespace(namespace)
    revision = revision_generator.next()
    affected = await _delete_vector_everywhere(item_id, namespace, revision)
    if affected == 0:
        raise HTTPException(status_code=404, detail="Vector not found")
    return {"status": "deleted", "id": item_id, "namespace": namespace}
