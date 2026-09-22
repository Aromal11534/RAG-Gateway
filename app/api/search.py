import json
import logging
from typing import Any, Dict, Literal, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.config import settings
from app.embeddings.adapter import embed_text
from app.search.fanout import search_all_shards
from app.search.merger import merge_and_sort

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/search", tags=["search"])


class SearchQuery(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    namespace: str = Field(min_length=1, max_length=128)
    query: str = Field(min_length=1)
    top_k: int = Field(default=5, ge=1)
    metadata_filter: Dict[str, Any] = Field(default_factory=dict, alias="filter")
    max_distance: Optional[float] = Field(default=None, ge=0, le=2)
    ranking: Literal["vector", "hybrid"] = "vector"
    lexical_weight: float = Field(default=0.25, ge=0, le=1)

    @field_validator("namespace", "query")
    @classmethod
    def reject_blank_strings(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value

    @field_validator("query")
    @classmethod
    def limit_query_length(cls, value: str) -> str:
        if len(value) > settings.max_text_length:
            raise ValueError("query is too long")
        return value

    @field_validator("top_k")
    @classmethod
    def limit_top_k(cls, value: int) -> int:
        if value > settings.max_top_k:
            raise ValueError(f"top_k must be at most {settings.max_top_k}")
        return value

    @field_validator("metadata_filter")
    @classmethod
    def validate_metadata_filter(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        if len(value) > 10:
            raise ValueError("filter supports at most 10 fields")
        if any(not key or len(key) > 128 for key in value):
            raise ValueError("filter keys must contain between 1 and 128 characters")
        encoded = json.dumps(value, ensure_ascii=False).encode("utf-8")
        if len(encoded) > settings.max_metadata_bytes:
            raise ValueError("filter is too large")
        return value


@router.post("")
@router.post("/", include_in_schema=False)
async def search_vectors_endpoint(req: SearchQuery):
    try:
        embedding = await embed_text(req.query)
    except Exception:
        logger.exception("Query embedding failed")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Embedding service is unavailable",
        ) from None

    candidate_k = min(
        settings.max_top_k,
        req.top_k * settings.search_candidate_multiplier,
    )
    all_results, searched_count, unavailable_count = await search_all_shards(
        req.namespace,
        embedding,
        candidate_k,
    )
    if searched_count == 0:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="No database shard completed the search",
        )

    return {
        "results": merge_and_sort(
            all_results,
            req.top_k,
            metadata_filter=req.metadata_filter,
            max_distance=req.max_distance,
            query=req.query if req.ranking == "hybrid" else None,
            lexical_weight=req.lexical_weight,
        ),
        "ranking": req.ranking,
        "candidate_count": len(all_results),
        "searched_shards": searched_count,
        "unavailable_shards": unavailable_count,
        "partial": unavailable_count > 0,
    }
