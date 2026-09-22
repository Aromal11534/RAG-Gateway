import pytest
from pydantic import ValidationError

from app.api.admin import ReinitializeRequest
from app.api.search import SearchQuery
from app.api.vectors import VectorItem


def test_search_rejects_blank_namespace_and_unbounded_top_k():
    with pytest.raises(ValidationError):
        SearchQuery(namespace="   ", query="hello")
    with pytest.raises(ValidationError):
        SearchQuery(namespace="tenant", query="hello", top_k=10_000)


def test_vector_metadata_defaults_are_not_shared():
    first = VectorItem(namespace="one", text="hello")
    second = VectorItem(namespace="two", text="world")
    first.metadata["changed"] = True
    assert second.metadata == {}


def test_cluster_reset_requires_exact_confirmation():
    with pytest.raises(ValidationError):
        ReinitializeRequest(confirm="yes")
    assert ReinitializeRequest(confirm="DELETE_ALL_VECTOR_DATA").confirm == "DELETE_ALL_VECTOR_DATA"
