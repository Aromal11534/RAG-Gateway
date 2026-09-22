import asyncio
import logging
import time
from typing import List, Tuple

from app.config import settings
from app.database.oracle import search_shard
from app.observability.metrics import metrics
from app.router.shard_registry import registry

logger = logging.getLogger(__name__)


async def _search_one(
    shard_id: str,
    namespace: str,
    embedding: List[float],
    top_k: int,
) -> Tuple[List[dict], bool]:
    started = time.perf_counter()
    try:
        results = await asyncio.wait_for(
            search_shard(shard_id, namespace, embedding, top_k),
            timeout=settings.shard_query_timeout_seconds,
        )
        registry.circuit_breaker.record_success(shard_id)
        metrics.observe_shard(shard_id, "success", time.perf_counter() - started)
        return results, True
    except Exception:
        registry.circuit_breaker.record_failure(shard_id)
        metrics.observe_shard(shard_id, "failure", time.perf_counter() - started)
        logger.exception("Search failed for shard %s", shard_id)
        return [], False


async def search_all_shards(
    namespace: str,
    embedding: List[float],
    top_k: int,
) -> tuple[list[dict], int, int]:
    """Search healthy shards concurrently and report only completed searches."""
    healthy_shards = registry.get_healthy_shards()
    if not healthy_shards:
        logger.error("No healthy shards are available for search")
        return [], 0, len(registry.shards)

    outcomes = await asyncio.gather(
        *(_search_one(shard_id, namespace, embedding, top_k) for shard_id in healthy_shards)
    )

    all_results: list[dict] = []
    successful = 0
    for results, succeeded in outcomes:
        if succeeded:
            successful += 1
            all_results.extend(results)

    unavailable = len(registry.shards) - successful
    return all_results, successful, unavailable
