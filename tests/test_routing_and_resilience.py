import asyncio

from app.health.circuit_breaker import CircuitBreaker
from app.router.consistent_hash import ConsistentHashRing
from app.search import fanout
from app.search.merger import merge_and_sort


def test_circuit_breaker_opens_and_recovers():
    now = [100.0]
    breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_seconds=10,
        clock=lambda: now[0],
    )

    breaker.record_failure("s1")
    assert breaker.is_healthy("s1")
    breaker.record_failure("s1")
    assert not breaker.is_healthy("s1")

    now[0] += 10
    assert breaker.is_healthy("s1")
    breaker.record_success("s1")
    assert breaker.status("s1")["failures"] == 0


def test_failed_half_open_probe_starts_a_new_cooldown():
    now = [100.0]
    breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_seconds=10,
        clock=lambda: now[0],
    )

    breaker.force_open("s1")
    now[0] += 10
    assert breaker.is_healthy("s1")

    breaker.record_failure("s1")
    assert not breaker.is_healthy("s1")
    assert breaker.status("s1")["retry_in_seconds"] == 10


def test_consistent_hash_applies_shard_weights():
    ring = ConsistentHashRing(replicas=10)
    ring.add_node("small", weight=1)
    ring.add_node("large", weight=2)

    assert len(ring._node_keys["small"]) == 10
    assert len(ring._node_keys["large"]) == 20
    assert set(ring.get_nodes("document-1")) == {"small", "large"}


def test_fanout_propagates_namespace_and_counts_only_successes(monkeypatch):
    calls = []

    async def fake_search(shard_id, namespace, embedding, top_k):
        calls.append((shard_id, namespace, embedding, top_k))
        if shard_id == "bad":
            raise RuntimeError("database unavailable")
        return [{"id": "1", "score": 0.1}]

    monkeypatch.setattr(fanout, "search_shard", fake_search)
    monkeypatch.setattr(fanout.registry, "get_healthy_shards", lambda: ["good", "bad"])
    monkeypatch.setattr(fanout.registry, "shards", {"good": object(), "bad": object()})

    results, searched, unavailable = asyncio.run(fanout.search_all_shards("tenant-a", [0.1], 3))

    assert all(call[1] == "tenant-a" for call in calls)
    assert results == [{"id": "1", "score": 0.1}]
    assert searched == 1
    assert unavailable == 1


def test_merger_deduplicates_fallback_copies_by_best_distance():
    merged = merge_and_sort(
        [
            {"id": "same", "score": 0.4, "shard_id": "a"},
            {"id": "same", "score": 0.2, "shard_id": "b"},
            {"id": "other", "score": 0.3, "shard_id": "a"},
        ],
        5,
    )
    assert [(item["id"], item["score"]) for item in merged] == [
        ("same", 0.2),
        ("other", 0.3),
    ]
