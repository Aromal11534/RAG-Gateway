import threading

from app.config import settings
from app.health.circuit_breaker import CircuitBreaker
from app.router.consistent_hash import ConsistentHashRing


class ShardRegistry:
    def __init__(self):
        self.shards = settings.get_shards()
        self.hash_ring = ConsistentHashRing()
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=settings.circuit_failure_threshold,
            recovery_seconds=settings.circuit_recovery_seconds,
        )
        self._drained: set[str] = set()
        self._lock = threading.RLock()

        for shard_id, config in self.shards.items():
            self.hash_ring.add_node(shard_id, config.weight)

    def get_healthy_shards(self) -> list[str]:
        return [shard_id for shard_id in self.shards if self.reserve(shard_id)]

    def reserve(self, shard_id: str) -> bool:
        with self._lock:
            if shard_id in self._drained:
                return False
        return self.circuit_breaker.allow_request(shard_id)

    def is_available(self, shard_id: str) -> bool:
        with self._lock:
            if shard_id in self._drained:
                return False
        return self.circuit_breaker.is_healthy(shard_id)

    def drain(self, shard_id: str) -> None:
        if shard_id not in self.shards:
            raise KeyError(shard_id)
        with self._lock:
            self._drained.add(shard_id)

    def activate(self, shard_id: str) -> None:
        if shard_id not in self.shards:
            raise KeyError(shard_id)
        with self._lock:
            self._drained.discard(shard_id)

    def is_drained(self, shard_id: str) -> bool:
        with self._lock:
            return shard_id in self._drained


registry = ShardRegistry()
