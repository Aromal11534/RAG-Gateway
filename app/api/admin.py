import asyncio
import logging
from typing import Awaitable, Callable, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, status
from pydantic import BaseModel, Field, field_validator

from app.config import settings
from app.consistency.revision import revision_generator
from app.database.oracle import clear_vectors
from app.database.oracle import delete_namespace as delete_namespace_data
from app.database.pools import get_pool_stats
from app.jobs.manager import job_manager
from app.rebalancing.worker import rebalance_vectors
from app.router.shard_registry import registry
from app.security import require_admin_key

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(require_admin_key)],
)


class ReinitializeRequest(BaseModel):
    confirm: Literal["DELETE_ALL_VECTOR_DATA"]


class PlacementRequest(BaseModel):
    namespace: str = Field(min_length=1, max_length=128)
    id: str = Field(min_length=1, max_length=512)


class RebalanceRequest(BaseModel):
    namespace: Optional[str] = Field(default=None, min_length=1, max_length=128)
    delete_extras: bool = False
    page_size: int = Field(default=250, ge=1, le=1_000)

    @field_validator("namespace")
    @classmethod
    def clean_namespace(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value


async def _run_cluster_operation(
    operation: Callable[[str], Awaitable[int]],
) -> int:
    healthy = registry.get_healthy_shards()
    if len(healthy) != len(registry.shards):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Administrative operations require every shard to be available",
        )

    async def run_one(shard_id: str):
        try:
            affected = await asyncio.wait_for(
                operation(shard_id),
                timeout=settings.shard_query_timeout_seconds,
            )
            registry.circuit_breaker.record_success(shard_id)
            return affected, True
        except Exception:
            registry.circuit_breaker.record_failure(shard_id)
            logger.exception("Administrative operation failed on shard %s", shard_id)
            return 0, False

    outcomes = await asyncio.gather(*(run_one(shard_id) for shard_id in healthy))
    if not all(succeeded for _, succeeded in outcomes):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Operation was only partially completed; retry is safe",
        )
    return sum(affected for affected, _ in outcomes)


@router.delete("/namespace/{namespace}")
async def delete_namespace(
    namespace: str = Path(min_length=1, max_length=128),
):
    namespace = namespace.strip()
    if not namespace:
        raise HTTPException(status_code=422, detail="namespace must not be blank")
    revision = revision_generator.next()
    affected = await _run_cluster_operation(
        lambda shard_id: delete_namespace_data(shard_id, namespace, revision)
    )
    return {
        "status": "namespace_deleted",
        "namespace": namespace,
        "deleted_vectors": affected,
    }


@router.post("/reinitialize")
async def reinitialize_cluster(_: ReinitializeRequest):
    affected = await _run_cluster_operation(clear_vectors)
    return {"status": "reinitialized", "deleted_vectors": affected}


@router.get("/shards")
async def list_shards():
    return {"shards": get_pool_stats()}


@router.post("/shards/{shard_id}/drain")
async def drain_shard(shard_id: str):
    if shard_id not in registry.shards:
        raise HTTPException(status_code=404, detail="Shard not found")
    active = [item for item in registry.shards if not registry.is_drained(item)]
    if shard_id in active and len(active) == 1:
        raise HTTPException(status_code=409, detail="Cannot drain the last active shard")
    registry.drain(shard_id)
    return {"status": "drained", "shard": shard_id, "persistent": False}


@router.post("/shards/{shard_id}/activate")
async def activate_shard(shard_id: str):
    if shard_id not in registry.shards:
        raise HTTPException(status_code=404, detail="Shard not found")
    registry.activate(shard_id)
    return {"status": "active", "shard": shard_id, "persistent": False}


@router.post("/placement")
async def inspect_placement(req: PlacementRequest):
    namespace = req.namespace.strip()
    item_id = req.id.strip()
    if not namespace or not item_id:
        raise HTTPException(status_code=422, detail="namespace and id must not be blank")
    placements = list(registry.hash_ring.get_nodes(f"{namespace}:{item_id}"))
    replica_count = min(settings.replication_factor, len(placements))
    return {
        "namespace": namespace,
        "id": item_id,
        "placements": [
            {
                "shard": shard_id,
                "intended_replica": index < replica_count,
                "available": registry.is_available(shard_id),
                "drained": registry.is_drained(shard_id),
            }
            for index, shard_id in enumerate(placements)
        ],
    }


@router.post("/rebalance", status_code=status.HTTP_202_ACCEPTED)
async def start_rebalance(req: RebalanceRequest):
    operation = job_manager.submit(
        "rebalance",
        {
            "namespace": req.namespace,
            "delete_extras": req.delete_extras,
            "page_size": req.page_size,
        },
    )
    return operation


job_manager.register(
    "rebalance",
    lambda p: rebalance_vectors(
        p.get("namespace"),
        p.get("delete_extras", True),
        p.get("page_size", 100),
    ),
)
