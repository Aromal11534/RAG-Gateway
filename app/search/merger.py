import re
from typing import Any


def _metadata_value(metadata: dict, dotted_key: str) -> Any:
    current: Any = metadata
    for part in dotted_key.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _matches_metadata(metadata: dict, filters: dict[str, Any]) -> bool:
    return all(_metadata_value(metadata, key) == value for key, value in filters.items())


def _terms(value: str) -> set[str]:
    return set(re.findall(r"\w+", value.casefold()))


def _lexical_score(query: str, text: str) -> float:
    query_terms = _terms(query)
    text_terms = _terms(text)
    if not query_terms or not text_terms:
        return 0.0
    return len(query_terms & text_terms) / len(query_terms)


def merge_and_sort(
    results: list[dict],
    top_k: int,
    *,
    metadata_filter: dict[str, Any] | None = None,
    max_distance: float | None = None,
    query: str | None = None,
    lexical_weight: float = 0.25,
) -> list[dict]:
    """Resolve replica versions, filter candidates, and rank global results."""
    latest_by_id: dict[str, dict] = {}
    for result in results:
        existing = latest_by_id.get(result["id"])
        result_revision = int(result.get("revision", 0))
        existing_revision = int(existing.get("revision", 0)) if existing else -1
        if (
            existing is None
            or result_revision > existing_revision
            or (result_revision == existing_revision and result["score"] < existing["score"])
        ):
            latest_by_id[result["id"]] = result

    filtered = []
    for result in latest_by_id.values():
        if max_distance is not None and result["score"] > max_distance:
            continue
        if metadata_filter and not _matches_metadata(result.get("metadata", {}), metadata_filter):
            continue
        filtered.append(dict(result))

    if query is None:
        return sorted(filtered, key=lambda item: item["score"])[:top_k]

    for result in filtered:
        vector_score = max(0.0, min(1.0, (2.0 - result["score"]) / 2.0))
        lexical_score = _lexical_score(query, result.get("text", ""))
        result["ranking_score"] = round(
            (1.0 - lexical_weight) * vector_score + lexical_weight * lexical_score,
            8,
        )
        result["lexical_score"] = round(lexical_score, 8)
    return sorted(filtered, key=lambda item: item["ranking_score"], reverse=True)[:top_k]
