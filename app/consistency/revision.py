import hashlib
import json
import threading
import time
from typing import Any


class RevisionGenerator:
    """Generate process-monotonic, time-sortable revisions."""

    def __init__(self) -> None:
        self._last = 0
        self._lock = threading.Lock()

    def next(self) -> int:
        with self._lock:
            candidate = time.time_ns()
            self._last = max(candidate, self._last + 1)
            return self._last


revision_generator = RevisionGenerator()


def content_hash(
    item_id: str,
    namespace: str,
    text: str,
    metadata: dict[str, Any],
) -> str:
    canonical = json.dumps(
        {
            "id": item_id,
            "namespace": namespace,
            "text": text,
            "metadata": metadata,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def newest(items: list[dict]) -> dict | None:
    if not items:
        return None
    return max(items, key=lambda item: (int(item.get("revision", 0)), item.get("shard_id", "")))
