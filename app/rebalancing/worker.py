import asyncio
import logging

from app.config import settings
from app.database.oracle import delete_vector, scan_vector_page, upsert_vector
from app.router.shard_registry import registry

logger = logging.getLogger(__name__)


async def rebalance_vectors(
    *,
    namespace: str | None = None,
    delete_extras: bool = False,
    page_size: int = 250,
) -> dict:
    """Ensure current ring replicas exist; optionally remove obsolete placements."""
    summary = {
        "scanned": 0,
        "replicas_written": 0,
        "extras_deleted": 0,
        "failures": 0,
        "namespace": namespace,
        "delete_extras": delete_extras,
    }

    for source_shard in registry.shards:
        after_namespace: str | None = None
        after_id: str | None = None
        while True:
            try:
                page = await asyncio.wait_for(
                    scan_vector_page(
                        source_shard,
                        namespace=namespace,
                        after_namespace=after_namespace,
                        after_id=after_id,
                        page_size=page_size,
                    ),
                    timeout=settings.shard_query_timeout_seconds,
                )
                registry.circuit_breaker.record_success(source_shard)
            except Exception:
                registry.circuit_breaker.record_failure(source_shard)
                logger.exception("Rebalance scan failed on shard %s", source_shard)
                summary["failures"] += 1
                break

            if not page:
                break

            for item in page:
                summary["scanned"] += 1
                placements = list(registry.hash_ring.get_nodes(f"{item['namespace']}:{item['id']}"))
                targets = placements[: min(settings.replication_factor, len(placements))]
                completed_targets: set[str] = set()

                for target_shard in targets:
                    if target_shard == source_shard:
                        completed_targets.add(target_shard)
                        continue
                    if not registry.reserve(target_shard):
                        summary["failures"] += 1
                        continue
                    try:
                        await asyncio.wait_for(
                            upsert_vector(
                                target_shard,
                                item["id"],
                                item["namespace"],
                                item["text"],
                                item["metadata"],
                                item["_embedding"],
                                revision=item["revision"],
                                content_hash=item.get("content_hash"),
                                document_id=item.get("document_id"),
                                chunk_index=item.get("chunk_index"),
                            ),
                            timeout=settings.shard_query_timeout_seconds,
                        )
                        registry.circuit_breaker.record_success(target_shard)
                        completed_targets.add(target_shard)
                        summary["replicas_written"] += 1
                    except Exception:
                        registry.circuit_breaker.record_failure(target_shard)
                        logger.exception(
                            "Rebalance write failed for %s on shard %s",
                            item["id"],
                            target_shard,
                        )
                        summary["failures"] += 1

                if (
                    delete_extras
                    and source_shard not in targets
                    and len(completed_targets) == len(targets)
                ):
                    try:
                        await asyncio.wait_for(
                            delete_vector(
                                source_shard,
                                item["id"],
                                item["namespace"],
                            ),
                            timeout=settings.shard_query_timeout_seconds,
                        )
                        summary["extras_deleted"] += 1
                    except Exception:
                        logger.exception(
                            "Could not remove obsolete copy of %s from %s",
                            item["id"],
                            source_shard,
                        )
                        summary["failures"] += 1

            last = page[-1]
            after_namespace = last["namespace"]
            after_id = last["id"]
            if len(page) < page_size:
                break

    return summary
