import asyncio
import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


import tempfile
import os

class JobManager:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or os.path.join(tempfile.gettempdir(), "jobs.sqlite3")
        self._handlers: Dict[str, Callable[[dict], Awaitable[Any]]] = {}
        self._init_db()
        self._tasks: set[asyncio.Task] = set()
        self._running = False
        self._worker_task: asyncio.Task | None = None

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                operation TEXT,
                payload TEXT,
                status TEXT,
                created_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                result TEXT,
                error TEXT,
                durable INTEGER
            )"""
            )
            # Reset running jobs to queued on startup
            conn.execute("UPDATE jobs SET status = 'queued' WHERE status = 'running'")

    def register(self, operation: str, handler: Callable[[dict], Awaitable[Any]]):
        self._handlers[operation] = handler

    def submit(self, operation: str, payload: dict) -> dict[str, Any]:
        job_id = str(uuid.uuid4())
        created_at = _now()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO jobs (id, operation, payload, status, created_at, durable) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (job_id, operation, json.dumps(payload), "queued", created_at, 1),
            )
        return {
            "id": job_id,
            "operation": operation,
            "status": "queued",
            "created_at": created_at,
            "durable": True,
        }

    def get(self, job_id: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return dict(row)

    async def _worker_loop(self):
        while self._running:
            try:
                job = await asyncio.to_thread(self._fetch_next_job)
                if not job:
                    await asyncio.sleep(1)
                    continue

                task = asyncio.create_task(self._run_job(job))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
            except Exception as e:
                logger.error("Job worker error: %s", e)
                await asyncio.sleep(5)

    def _fetch_next_job(self) -> dict | None:
        with sqlite3.connect(self.db_path, isolation_level="EXCLUSIVE") as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1"
            )
            row = cursor.fetchone()
            if row:
                conn.execute(
                    "UPDATE jobs SET status = 'running', started_at = ? WHERE id = ?",
                    (_now(), row["id"]),
                )
                conn.commit()
                return dict(row)
        return None

    async def _run_job(self, job: dict):
        job_id = job["id"]
        operation = job["operation"]
        payload = json.loads(job["payload"])

        handler = self._handlers.get(operation)
        if not handler:
            await self._update_job(job_id, "failed", error=f"No handler for operation: {operation}")
            return

        try:
            result = await handler(payload)
            result_str = json.dumps(result) if result else None
            await self._update_job(job_id, "completed", result=result_str)
        except Exception as e:
            logger.exception("Job %s failed", job_id)
            await self._update_job(job_id, "failed", error=str(e))

    async def _update_job(
        self, job_id: str, status: str, result: str | None = None, error: str | None = None
    ):
        def _update():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE jobs SET status = ?, finished_at = ?, result = ?, error = ? "
                    "WHERE id = ?",
                    (status, _now(), result, error, job_id),
                )

        await asyncio.to_thread(_update)

    def start(self):
        if not self._running:
            self._running = True
            self._worker_task = asyncio.create_task(self._worker_loop())

    async def shutdown(self):
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)


job_manager = JobManager()
