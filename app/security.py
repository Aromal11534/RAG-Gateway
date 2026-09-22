import secrets
from typing import Optional

from fastapi import Header, HTTPException, status
from pydantic import SecretStr
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.config import settings

API_KEY_HEADER = "X-API-Key"
ADMIN_KEY_HEADER = "X-Admin-API-Key"
PUBLIC_PATHS = frozenset({"/", "/health", "/live", "/ready"})


def _matches(candidate: Optional[str], expected: Optional[SecretStr]) -> bool:
    if not candidate or expected is None:
        return False
    expected_value = expected.get_secret_value()
    return bool(expected_value) and secrets.compare_digest(
        candidate.encode("utf-8"),
        expected_value.encode("utf-8"),
    )


async def authentication_middleware(request: Request, call_next) -> Response:
    """Fail-closed authentication boundary for every non-public route."""
    if request.method == "OPTIONS" or request.url.path in PUBLIC_PATHS:
        return await call_next(request)

    if settings.gateway_api_key is None:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": "API authentication is not configured"},
        )

    if not _matches(request.headers.get(API_KEY_HEADER), settings.gateway_api_key):
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"detail": "Invalid or missing API key"},
            headers={"WWW-Authenticate": "ApiKey"},
        )

    return await call_next(request)


async def require_admin_key(
    admin_key: Optional[str] = Header(default=None, alias=ADMIN_KEY_HEADER),
) -> None:
    """Require a separate capability for destructive administrative routes."""
    if settings.admin_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Administrative access is not configured",
        )
    if not _matches(admin_key, settings.admin_api_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Administrative access denied",
        )
