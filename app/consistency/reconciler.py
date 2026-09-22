import asyncio
import logging

from app.config import settings
from app.database.oracle import upsert_vector, delete_vector as delete_vector_from_shard
from app.router.shard_registry import registry

logger = logging.getLogger(__name__)


async def repair_vector_replicas(
    latest: dict,
    target_shards: list[str],
    observed: dict[str, dict | None],
) -> int:
    """Copy the newest observed value to missing or stale intended replicas."""
    latest_revision = int(latest.get("revision", 0))
    stale_targets = [
        shard_id
        for shard_id in target_shards
        if observed.get(shard_id) is None
        or int(observed[shard_id].get("revision", 0)) < latest_revision
    ]

    async def repair_one(shard_id: str) -> bool:
        if not registry.is_available(shard_id):
            return False
        try:
            if latest.get("is_deleted"):
                await asyncio.wait_for(
                    delete_vector_from_shard(
                        shard_id, latest["id"], latest["namespace"], latest_revision
                    ),
                    timeout=settings.shard_query_timeout_seconds,
                )
            else:
                await asyncio.wait_for(
                    upsert_vector(
                        shard_id,
                        latest["id"],
                        latest["namespace"],
                        latest["text"],
                        latest["metadata"],
                        latest["_embedding"],
                        revision=latest_revision,
                        content_hash=latest.get("content_hash"),
                        document_id=latest.get("document_id"),
                        chunk_index=latest.get("chunk_index"),
                    ),
                    timeout=settings.shard_query_timeout_seconds,
                )
            registry.circuit_breaker.record_success(shard_id)
            return True
        except Exception:
            registry.circuit_breaker.record_failure(shard_id)
            logger.exception("Read repair failed on shard %s", shard_id)
            return False

    if not stale_targets:
        return 0
    outcomes = await asyncio.gather(*(repair_one(shard_id) for shard_id in stale_targets))
    return sum(outcomes)
