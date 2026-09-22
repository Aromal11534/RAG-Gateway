import asyncio
import logging
from typing import Any, Dict

import oracledb

from app.config import settings

logger = logging.getLogger(__name__)
pools: Dict[str, Any] = {}


async def initialize_pools() -> Dict[str, bool]:
    from app.router.shard_registry import registry

    shards_config = settings.get_shards()
    for shard_id, missing in settings.get_incomplete_shards().items():
        logger.warning(
            "Ignoring incomplete shard %s; missing fields: %s",
            shard_id,
            ", ".join(missing),
        )
    if not shards_config:
        raise RuntimeError("No complete Oracle shard configuration was found")

    outcomes: Dict[str, bool] = {}
    for shard_id, config in shards_config.items():
        try:
            pool = oracledb.create_pool_async(
                user=config.user,
                password=config.password.get_secret_value(),
                dsn=config.dsn,
                min=settings.db_pool_min,
                max=settings.db_pool_max,
                increment=settings.db_pool_increment,
                getmode=oracledb.POOL_GETMODE_TIMEDWAIT,
                wait_timeout=settings.db_pool_wait_timeout_ms,
            )
            pools[shard_id] = pool
            registry.circuit_breaker.record_success(shard_id)
            outcomes[shard_id] = True
            logger.info("Initialized connection pool for shard %s", shard_id)
        except Exception:
            registry.circuit_breaker.force_open(shard_id)
            outcomes[shard_id] = False
            logger.exception("Failed to initialize pool for shard %s", shard_id)

    if not any(outcomes.values()):
        raise RuntimeError("No Oracle shard connection pool could be initialized")
    return outcomes


async def close_pools() -> None:
    for shard_id, pool in list(pools.items()):
        try:
            await pool.close()
            logger.info("Closed pool for shard %s", shard_id)
        except Exception:
            logger.exception("Error closing pool for shard %s", shard_id)
        finally:
            pools.pop(shard_id, None)


def get_pool(shard_id: str):
    return pools.get(shard_id)


async def ping_shard(shard_id: str) -> bool:
    from app.router.shard_registry import registry

    pool = get_pool(shard_id)
    if pool is None:
        registry.circuit_breaker.force_open(shard_id)
        return False

    try:
        async with asyncio.timeout(settings.shard_query_timeout_seconds):
            async with pool.acquire() as connection:
                await connection.ping()
        registry.circuit_breaker.record_success(shard_id)
        return True
    except Exception:
        registry.circuit_breaker.record_failure(shard_id)
        logger.warning("Health probe failed for shard %s", shard_id, exc_info=True)
        return False


def get_pool_stats() -> Dict[str, dict]:
    from app.router.shard_registry import registry

    stats: Dict[str, dict] = {}
    for shard_id in registry.shards:
        pool = get_pool(shard_id)
        stats[shard_id] = {
            "configured": True,
            "pool_initialized": pool is not None,
            "open_connections": getattr(pool, "opened", 0) if pool else 0,
            "busy_connections": getattr(pool, "busy", 0) if pool else 0,
            "circuit": registry.circuit_breaker.status(shard_id),
            "drained": registry.is_drained(shard_id),
            "weight": registry.shards[shard_id].weight,
        }
    return stats
