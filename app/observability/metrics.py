import threading
from collections import defaultdict


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class MetricsRegistry:
    def __init__(self) -> None:
        self._request_count: dict[tuple[str, str, int], int] = defaultdict(int)
        self._request_duration: dict[tuple[str, str], float] = defaultdict(float)
        self._shard_count: dict[tuple[str, str], int] = defaultdict(int)
        self._shard_duration: dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()

    def observe_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_seconds: float,
    ) -> None:
        with self._lock:
            self._request_count[(method, path, status_code)] += 1
            self._request_duration[(method, path)] += duration_seconds

    def observe_shard(self, shard_id: str, outcome: str, duration_seconds: float) -> None:
        with self._lock:
            self._shard_count[(shard_id, outcome)] += 1
            self._shard_duration[shard_id] += duration_seconds

    def render(self) -> str:
        lines = [
            "# HELP gateway_requests_total HTTP requests processed.",
            "# TYPE gateway_requests_total counter",
        ]
        with self._lock:
            for (method, path, status_code), value in sorted(self._request_count.items()):
                lines.append(
                    "gateway_requests_total{"
                    f'method="{_escape(method)}",path="{_escape(path)}",'
                    f'status="{status_code}"}} {value}'
                )
            lines.extend(
                [
                    "# HELP gateway_request_duration_seconds_total "
                    "Cumulative HTTP request duration.",
                    "# TYPE gateway_request_duration_seconds_total counter",
                ]
            )
            for (method, path), value in sorted(self._request_duration.items()):
                lines.append(
                    "gateway_request_duration_seconds_total{"
                    f'method="{_escape(method)}",path="{_escape(path)}"}} {value:.9f}'
                )
            lines.extend(
                [
                    "# HELP gateway_shard_operations_total Shard operations by outcome.",
                    "# TYPE gateway_shard_operations_total counter",
                ]
            )
            for (shard_id, outcome), value in sorted(self._shard_count.items()):
                lines.append(
                    "gateway_shard_operations_total{"
                    f'shard="{_escape(shard_id)}",outcome="{_escape(outcome)}"}} {value}'
                )
            lines.extend(
                [
                    "# HELP gateway_shard_duration_seconds_total "
                    "Cumulative shard operation duration.",
                    "# TYPE gateway_shard_duration_seconds_total counter",
                ]
            )
            for shard_id, value in sorted(self._shard_duration.items()):
                lines.append(
                    "gateway_shard_duration_seconds_total{"
                    f'shard="{_escape(shard_id)}"}} {value:.9f}'
                )
        return "\n".join(lines) + "\n"


metrics = MetricsRegistry()
