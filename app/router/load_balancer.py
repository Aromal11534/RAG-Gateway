from app.router.diverter import route_write


def get_target_shard(key: str, namespace: str | None = None) -> str:
    """Backward-compatible entry point for deterministic write routing."""
    return route_write(key, namespace)
