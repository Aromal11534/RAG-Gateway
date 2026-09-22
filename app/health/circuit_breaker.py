import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional


@dataclass
class _ShardState:
    failures: int = 0
    opened_at: Optional[float] = None
    half_open_in_flight: bool = False


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self._clock = clock
        self._states: Dict[str, _ShardState] = {}
        self._lock = threading.RLock()

    def record_failure(self, shard_id: str) -> None:
        with self._lock:
            state = self._states.setdefault(shard_id, _ShardState())
            now = self._clock()
            if state.opened_at is not None:
                # A failed half-open probe must begin a fresh cooldown. Keeping
                # the old timestamp would leave the circuit permanently open to
                # traffic after its first recovery interval.
                if now - state.opened_at >= self.recovery_seconds:
                    state.failures = self.failure_threshold
                    state.opened_at = now
                    state.half_open_in_flight = False
                return
            state.failures += 1
            if state.failures >= self.failure_threshold:
                state.opened_at = now

    def force_open(self, shard_id: str) -> None:
        """Immediately remove a known-unavailable shard from routing."""
        with self._lock:
            self._states[shard_id] = _ShardState(
                failures=self.failure_threshold,
                opened_at=self._clock(),
            )

    def record_success(self, shard_id: str) -> None:
        with self._lock:
            self._states.pop(shard_id, None)

    def is_healthy(self, shard_id: str) -> bool:
        with self._lock:
            state = self._states.get(shard_id)
            if state is None or state.opened_at is None:
                return True
            # After the recovery interval, allow a probe request through.
            return self._clock() - state.opened_at >= self.recovery_seconds

    def allow_request(self, shard_id: str) -> bool:
        """Reserve a request slot, allowing only one half-open probe at a time."""
        with self._lock:
            state = self._states.get(shard_id)
            if state is None or state.opened_at is None:
                return True
            if self._clock() - state.opened_at < self.recovery_seconds:
                return False
            if state.half_open_in_flight:
                return False
            state.half_open_in_flight = True
            return True

    def status(self, shard_id: str) -> dict:
        with self._lock:
            state = self._states.get(shard_id, _ShardState())
            healthy = state.opened_at is None
            retry_in = 0.0
            if state.opened_at is not None:
                retry_in = max(
                    0.0,
                    self.recovery_seconds - (self._clock() - state.opened_at),
                )
            return {
                "healthy": healthy or retry_in == 0,
                "failures": state.failures,
                "retry_in_seconds": round(retry_in, 3),
                "half_open_probe_in_flight": state.half_open_in_flight,
            }
