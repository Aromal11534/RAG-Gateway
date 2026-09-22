import array
import asyncio

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.api import vectors
from app.api.documents import DocumentIngestRequest
from app.config import Settings
from app.consistency.revision import RevisionGenerator, content_hash, newest
from app.database import oracle
from app.embeddings import adapter
from app.health.circuit_breaker import CircuitBreaker
from app.ingestion.chunker import chunk_text
from app.middleware import RequestBodyLimitMiddleware, operational_middleware
from app.search.merger import merge_and_sort
from app.security import _matches


def test_revision_generation_and_content_hash_are_deterministic():
    generator = RevisionGenerator()
    first = generator.next()
    second = generator.next()

    assert second > first
    assert content_hash("1", "n", "text", {"b": 2, "a": 1}) == content_hash(
        "1", "n", "text", {"a": 1, "b": 2}
    )


def test_newest_and_search_merger_prefer_latest_replica_over_nearest_stale_copy():
    stale = {"id": "same", "revision": 1, "score": 0.1, "text": "old"}
    current = {"id": "same", "revision": 2, "score": 0.8, "text": "new"}

    assert newest([stale, current]) == current
    assert merge_and_sort([stale, current], 5) == [current]


def test_hybrid_ranking_and_metadata_filtering():
    results = [
        {
            "id": "vector-near",
            "revision": 1,
            "score": 0.1,
            "text": "unrelated material",
            "metadata": {"type": "policy"},
        },
        {
            "id": "lexical-match",
            "revision": 1,
            "score": 0.5,
            "text": "refund policy and process",
            "metadata": {"type": "policy"},
        },
        {
            "id": "filtered",
            "revision": 1,
            "score": 0.01,
            "text": "refund policy",
            "metadata": {"type": "private"},
        },
    ]

    ranked = merge_and_sort(
        results,
        5,
        metadata_filter={"type": "policy"},
        query="refund policy",
        lexical_weight=0.8,
    )

    assert [item["id"] for item in ranked] == ["lexical-match", "vector-near"]
    assert all("ranking_score" in item for item in ranked)


def test_chunker_produces_overlapping_chunks_with_source_offsets():
    text = "alpha beta gamma delta epsilon zeta eta theta"
    chunks = chunk_text(text, size=20, overlap=5)

    assert len(chunks) > 1
    assert all(text[item.start_char : item.end_char] == item.text for item in chunks)
    assert chunks[1].start_char < chunks[0].end_char


def test_document_validation_rejects_invalid_overlap():
    with pytest.raises(ValidationError):
        DocumentIngestRequest(
            namespace="tenant",
            text="document",
            chunk_size=100,
            chunk_overlap=100,
        )


def test_settings_validate_relationships_and_report_partial_shards():
    with pytest.raises(ValidationError):
        Settings(_env_file=None, db_pool_min=2, db_pool_max=1)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, gateway_api_key="short")

    configured = Settings(_env_file=None, db1_user="user")
    assert configured.get_incomplete_shards() == {"oracle_01": ["password", "dsn"]}


def test_non_ascii_api_key_is_rejected_without_raising():
    from pydantic import SecretStr

    assert not _matches("é", SecretStr("expected-value"))


def test_half_open_circuit_allows_only_one_probe():
    now = [100.0]
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_seconds=10,
        clock=lambda: now[0],
    )
    breaker.record_failure("s1")
    now[0] += 10

    assert breaker.allow_request("s1")
    assert not breaker.allow_request("s1")
    breaker.record_success("s1")
    assert breaker.allow_request("s1")


def test_batch_endpoint_reports_individual_failures(monkeypatch):
    async def fake_write(item, *, upsert, embedding=None):
        if item.id == "bad":
            raise HTTPException(status_code=503, detail="unavailable")
        return {"id": item.id, "status": "upserted", "partial": False}

    monkeypatch.setattr(vectors, "_write", fake_write)

    async def fake_embed_texts(texts):
        return [[0.1] for _ in texts]

    monkeypatch.setattr(vectors, "embed_texts", fake_embed_texts)
    app = FastAPI()
    app.include_router(vectors.router)
    response = TestClient(app).post(
        "/vectors/batch",
        json={
            "items": [
                {"id": "good", "namespace": "n", "text": "a"},
                {"id": "bad", "namespace": "n", "text": "b"},
            ]
        },
    )

    assert response.status_code == 207
    assert response.json()["succeeded"] == 1
    assert response.json()["failed"] == 1


def test_operational_middleware_adds_request_id_and_limits_body(monkeypatch):
    from app import middleware

    monkeypatch.setattr(middleware.settings, "max_request_body_bytes", 4)
    monkeypatch.setattr(middleware.settings, "rate_limit_requests_per_minute", 0)
    app = FastAPI()
    app.middleware("http")(operational_middleware)

    @app.post("/echo")
    async def echo():
        return {"ok": True}

    client = TestClient(app)
    accepted = client.post("/echo", content="1234")
    rejected = client.post("/echo", content="12345")

    assert accepted.status_code == 200
    assert accepted.headers["X-Request-ID"]
    assert rejected.status_code == 413


def test_streamed_request_body_limit_does_not_require_content_length(monkeypatch):
    from app import middleware

    monkeypatch.setattr(middleware.settings, "max_request_body_bytes", 4)
    monkeypatch.setattr(middleware.settings, "rate_limit_requests_per_minute", 0)
    app = FastAPI()
    app.middleware("http")(operational_middleware)
    app.add_middleware(RequestBodyLimitMiddleware)

    @app.post("/consume")
    async def consume(request: Request):
        return {"size": len(await request.body())}

    response = TestClient(app).post(
        "/consume",
        content=iter([b"123", b"45"]),
    )
    assert response.status_code == 413


def test_background_job_lifecycle():
    from app.jobs.manager import JobManager

    async def scenario():
        manager = JobManager()

        async def work():
            await asyncio.sleep(0)
            return {"done": True}

        job = manager.submit("test", work())
        await asyncio.gather(*manager._tasks)
        completed = manager.get(job["id"])
        assert completed["status"] == "completed"
        assert completed["result"] == {"done": True}

    asyncio.run(scenario())


def test_schema_queries_include_revision_guards_and_bound_namespaces():
    assert "WHERE :revision >= target.revision" in oracle.UPSERT_QUERY
    assert "namespace = :namespace" in oracle.SCAN_VECTORS_QUERY
    assert ":after_namespace" in oracle.SCAN_VECTORS_QUERY


def test_metadata_decoder_accepts_native_oracle_json_objects():
    metadata = {"source": "oracle-json", "nested": {"chunk": 1}}

    assert asyncio.run(oracle._decode_metadata(metadata)) == metadata


def test_oracle_embedding_provider_discovers_model_and_embeds(monkeypatch):
    class FakeCursor:
        def __init__(self):
            self.row = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def execute(self, query, binds=None):
            if "user_mining_models" in query:
                self.row = ("DB_EMBED_MODEL",)
            else:
                self.row = (array.array("f", [0.25] * 384),)

        async def fetchone(self):
            return self.row

    class FakeConnection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def cursor(self):
            return FakeCursor()

    class FakePool:
        def acquire(self):
            return FakeConnection()

    monkeypatch.setattr(adapter, "_model_name", None)
    monkeypatch.setattr(adapter, "_source_shard", None)
    monkeypatch.setattr(adapter, "_loaded_dimension", None)
    monkeypatch.setattr(adapter.settings, "oracle_embedding_model", None)
    monkeypatch.setattr(adapter.settings, "oracle_embedding_shard", "oracle_04")
    monkeypatch.setattr(adapter.registry, "shards", {"oracle_04": object()})
    monkeypatch.setattr(adapter.registry, "reserve", lambda _: True)
    monkeypatch.setattr(adapter, "get_pool", lambda _: FakePool())

    async def scenario():
        await adapter.initialize_model()
        embedding = await adapter.embed_text("hello from Oracle")
        assert len(embedding) == 384
        assert adapter.model_info()["model"] == "DB_EMBED_MODEL"
        assert adapter.model_info()["hosted_in_gateway"] is False

    asyncio.run(scenario())
