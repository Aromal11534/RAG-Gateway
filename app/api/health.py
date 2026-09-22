import asyncio

from fastapi import APIRouter, Response, status

from app.database.pools import get_pool_stats, ping_shard
from app.router.shard_registry import registry

router = APIRouter(tags=["health"])


async def _readiness(response: Response):
    shard_ids = list(registry.shards)

    async def check(shard_id: str) -> bool:
        if registry.is_drained(shard_id):
            return False
        return await ping_shard(shard_id)

    checks = await asyncio.gather(*(check(shard_id) for shard_id in shard_ids))
    healthy = sum(checks)
    total = len(shard_ids)

    if total > 0 and healthy == total:
        service_status = "healthy"
    elif healthy > 0:
        service_status = "degraded"
    else:
        service_status = "unhealthy"
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": service_status,
        "healthy_shards": healthy,
        "unavailable_shards": total - healthy,
    }


@router.get("/live")
async def get_liveness():
    return {"status": "alive"}


@router.get("/ready")
async def get_readiness(response: Response):
    return await _readiness(response)


@router.get("/health")
async def get_health(response: Response):
    return await _readiness(response)


@router.get("/stats")
async def get_stats():
    return {"shards": get_pool_stats()}
