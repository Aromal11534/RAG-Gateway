import asyncio

from app.database import oracle


class FakeAsyncLob:
    def __init__(self, value):
        self.value = value

    async def read(self):
        return self.value


class FakeCursor:
    def __init__(self):
        self.query = None
        self.binds = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def execute(self, query, binds):
        self.query = query
        self.binds = binds

    async def fetchall(self):
        return [
            (
                "vector-1",
                0.125,
                FakeAsyncLob("allowed text"),
                FakeAsyncLob('{"source":"test"}'),
            )
        ]


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def cursor(self):
        return self._cursor


class FakePool:
    def __init__(self, cursor):
        self._connection = FakeConnection(cursor)

    def acquire(self):
        return self._connection


def test_search_enforces_namespace_at_the_sql_boundary(monkeypatch):
    cursor = FakeCursor()
    monkeypatch.setattr(oracle, "get_pool", lambda _: FakePool(cursor))

    malicious_namespace = "tenant-a' OR 1=1 --"
    results = asyncio.run(
        oracle.search_shard(
            "oracle_01",
            malicious_namespace,
            [0.1, 0.2],
            5,
        )
    )

    assert "WHERE namespace = :namespace" in cursor.query
    assert malicious_namespace not in cursor.query
    assert cursor.binds["namespace"] == malicious_namespace
    assert results[0]["text"] == "allowed text"
    assert results[0]["metadata"] == {"source": "test"}


def test_search_sql_returns_the_computed_distance():
    assert "VECTOR_DISTANCE" in oracle.VECTOR_SEARCH_QUERY
    assert "AS score" in oracle.VECTOR_SEARCH_QUERY
    assert "SELECT id, score" not in oracle.VECTOR_SEARCH_QUERY
