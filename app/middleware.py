import hashlib
import math
import re
import threading
import time
import uuid
from collections import defaultdict, deque

from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.config import settings
from app.observability.metrics import metrics
from app.security import PUBLIC_PATHS

REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")


class SlidingWindowLimiter:
    def __init__(self) -> None:
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key: str, limit: int, now: float) -> tuple[bool, int]:
        with self._lock:
            timestamps = self._requests[key]
            cutoff = now - 60.0
            while timestamps and timestamps[0] <= cutoff:
                timestamps.popleft()
            if len(timestamps) >= limit:
                retry_after = max(1, math.ceil(60.0 - (now - timestamps[0])))
                return False, retry_after
            timestamps.append(now)
            return True, 0


limiter = SlidingWindowLimiter()


class RequestBodyTooLarge(Exception):
    pass


class RequestBodyLimitMiddleware:
    """Enforce the body limit even when Content-Length is absent or incorrect."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        received = 0

        async def limited_receive():
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > settings.max_request_body_bytes:
                    raise RequestBodyTooLarge
            return message

        try:
            await self.app(scope, limited_receive, send)
        except RequestBodyTooLarge:
            response = JSONResponse(
                status_code=413,
                content={"detail": "Request body is too large"},
            )
            await response(scope, receive, send)


def _rate_limit_key(request: Request) -> str:
    credential = request.headers.get("X-API-Key")
    if credential:
        return hashlib.sha256(credential.encode("utf-8")).hexdigest()
    return request.client.host if request.client else "unknown"


async def operational_middleware(request: Request, call_next) -> Response:
    started = time.perf_counter()
    request_id = request.headers.get("X-Request-ID", "")
    if not REQUEST_ID_PATTERN.fullmatch(request_id):
        request_id = str(uuid.uuid4())
    request.state.request_id = request_id

    content_length = request.headers.get("content-length")
    if content_length and content_length.isdigit():
        if int(content_length) > settings.max_request_body_bytes:
            response: Response = JSONResponse(
                status_code=413,
                content={"detail": "Request body is too large"},
            )
            response.headers["X-Request-ID"] = request_id
            metrics.observe_request(
                request.method,
                request.url.path,
                response.status_code,
                time.perf_counter() - started,
            )
            return response

    if settings.rate_limit_requests_per_minute > 0 and request.url.path not in PUBLIC_PATHS:
        allowed, retry_after = limiter.check(
            _rate_limit_key(request),
            settings.rate_limit_requests_per_minute,
            time.monotonic(),
        )
        if not allowed:
            response = JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )
            response.headers["X-Request-ID"] = request_id
            metrics.observe_request(
                request.method,
                request.url.path,
                response.status_code,
                time.perf_counter() - started,
            )
            return response

    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        response.headers["X-Request-ID"] = request_id
        return response
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", request.url.path)
        metrics.observe_request(
            request.method,
            path,
            status_code,
            time.perf_counter() - started,
        )
