import logging

from app.router.shard_registry import registry

logger = logging.getLogger(__name__)


def route_write(item_id: str, namespace: str | None = None) -> str:
    """Select the first healthy shard clockwise from the deterministic owner."""
    routing_key = f"{namespace}:{item_id}" if namespace is not None else item_id
    candidates = list(registry.hash_ring.get_nodes(routing_key))
    if not candidates:
        raise ValueError("No shards are configured")

    primary = candidates[0]
    for shard_id in candidates:
        if registry.reserve(shard_id):
            if shard_id != primary:
                logger.warning(
                    "Primary shard %s is unavailable; routing write to %s",
                    primary,
                    shard_id,
                )
            return shard_id

    raise RuntimeError("All configured shards are unavailable")
