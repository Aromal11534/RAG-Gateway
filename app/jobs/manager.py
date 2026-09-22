import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobManager:
    def __init__(self, max_history: int = 1_000) -> None:
        self.max_history = max_history
        self._jobs: dict[str, dict[str, Any]] = {}
        self._tasks: set[asyncio.Task] = set()

    def submit(self, operation: str, awaitable: Awaitable[Any]) -> dict[str, Any]:
        job_id = str(uuid.uuid4())
        record: dict[str, Any] = {
            "id": job_id,
            "operation": operation,
            "status": "queued",
            "created_at": _now(),
            "started_at": None,
            "finished_at": None,
            "result": None,
            "error": None,
            "durable": False,
        }
        self._jobs[job_id] = record

        async def run() -> None:
            record["status"] = "running"
            record["started_at"] = _now()
            try:
                record["result"] = await awaitable
                record["status"] = "completed"
            except asyncio.CancelledError:
                record["status"] = "cancelled"
                raise
            except Exception as exc:
                record["status"] = "failed"
                record["error"] = str(exc)
            finally:
                record["finished_at"] = _now()

        task = asyncio.create_task(run(), name=f"gateway-job-{job_id}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        self._trim_history()
        return dict(record)

    def get(self, job_id: str) -> dict[str, Any] | None:
        record = self._jobs.get(job_id)
        return dict(record) if record else None

    def _trim_history(self) -> None:
        completed = [
            job_id
            for job_id, record in self._jobs.items()
            if record["status"] in {"completed", "failed", "cancelled"}
        ]
        excess = max(0, len(self._jobs) - self.max_history)
        for job_id in completed[:excess]:
            self._jobs.pop(job_id, None)

    async def shutdown(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)


job_manager = JobManager()
