from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import settings
from app.security import authentication_middleware, require_admin_key


def build_test_app() -> FastAPI:
    app = FastAPI()
    app.middleware("http")(authentication_middleware)

    @app.get("/")
    async def root():
        return {"ok": True}

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.get("/protected")
    async def protected():
        return {"ok": True}

    @app.post("/admin/action", dependencies=[Depends(require_admin_key)])
    async def admin_action():
        return {"ok": True}

    return app


def test_shared_authentication_boundary_rejects_missing_and_invalid_keys(monkeypatch):
    monkeypatch.setattr(settings, "gateway_api_key", SecretStr("gateway-secret"))
    client = TestClient(build_test_app())

    assert client.get("/protected").status_code == 401
    assert client.get("/protected", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/protected", headers={"X-API-Key": "gateway-secret"}).status_code == 200


def test_public_probe_routes_remain_available(monkeypatch):
    monkeypatch.setattr(settings, "gateway_api_key", SecretStr("gateway-secret"))
    client = TestClient(build_test_app())

    assert client.get("/").status_code == 200
    assert client.get("/health").status_code == 200


def test_missing_server_key_fails_closed(monkeypatch):
    monkeypatch.setattr(settings, "gateway_api_key", None)
    response = TestClient(build_test_app()).get("/protected")
    assert response.status_code == 503


def test_admin_requires_both_gateway_and_distinct_admin_keys(monkeypatch):
    monkeypatch.setattr(settings, "gateway_api_key", SecretStr("gateway-secret"))
    monkeypatch.setattr(settings, "admin_api_key", SecretStr("admin-secret"))
    client = TestClient(build_test_app())

    assert client.post("/admin/action").status_code == 401
    assert (
        client.post(
            "/admin/action",
            headers={"X-API-Key": "gateway-secret"},
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/admin/action",
            headers={
                "X-API-Key": "gateway-secret",
                "X-Admin-API-Key": "wrong",
            },
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/admin/action",
            headers={
                "X-API-Key": "gateway-secret",
                "X-Admin-API-Key": "admin-secret",
            },
        ).status_code
        == 200
    )
