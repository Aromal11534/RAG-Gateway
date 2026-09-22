import bisect
import hashlib
from typing import Dict, Iterator, List


class ConsistentHashRing:
    def __init__(self, replicas: int = 100):
        self.replicas = replicas
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        self._node_keys: Dict[str, List[int]] = {}

    def _hash(self, key: str) -> int:
        # This is distribution, not cryptographic authentication; MD5 is adequate here.
        return int(hashlib.md5(key.encode("utf-8"), usedforsecurity=False).hexdigest(), 16)

    def add_node(self, node: str, weight: float = 1.0) -> None:
        if weight <= 0:
            raise ValueError("Shard weight must be positive")
        if node in self._node_keys:
            self.remove_node(node)

        keys: List[int] = []
        virtual_nodes = max(1, round(self.replicas * weight))
        for i in range(virtual_nodes):
            key = self._hash(f"{node}:{i}")
            # A collision is extremely unlikely, but skipping it keeps ring bookkeeping sound.
            if key in self.ring:
                continue
            self.ring[key] = node
            bisect.insort(self.sorted_keys, key)
            keys.append(key)
        self._node_keys[node] = keys

    def remove_node(self, node: str) -> None:
        for key in self._node_keys.pop(node, []):
            self.ring.pop(key, None)
            index = bisect.bisect_left(self.sorted_keys, key)
            if index < len(self.sorted_keys) and self.sorted_keys[index] == key:
                self.sorted_keys.pop(index)

    def get_node(self, item_key: str) -> str | None:
        return next(self.get_nodes(item_key), None)

    def get_nodes(self, item_key: str) -> Iterator[str]:
        """Yield unique nodes clockwise from the item's position on the ring."""
        if not self.sorted_keys:
            return

        start = bisect.bisect_right(self.sorted_keys, self._hash(item_key))
        seen = set()
        for offset in range(len(self.sorted_keys)):
            key = self.sorted_keys[(start + offset) % len(self.sorted_keys)]
            node = self.ring[key]
            if node not in seen:
                seen.add(node)
                yield node
