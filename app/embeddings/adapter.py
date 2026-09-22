import logging
from typing import Any

from app.config import settings
from app.database.pools import get_pool
from app.router.shard_registry import registry

logger = logging.getLogger(__name__)
_model_name: str | None = None
_source_shard: str | None = None
_loaded_dimension: int | None = None


def _quoted_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def _candidate_shards() -> list[str]:
    configured = settings.oracle_embedding_shard
    ordered = list(registry.shards)
    if configured:
        if configured not in registry.shards:
            raise RuntimeError(f"Oracle embedding shard {configured} is not configured")
        ordered.remove(configured)
        ordered.insert(0, configured)
    return ordered


async def _discover_model(shard_id: str) -> tuple[str, int]:
    pool = get_pool(shard_id)
    if pool is None:
        raise RuntimeError(f"No connection pool is available for shard {shard_id}")

    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            model_name = settings.oracle_embedding_model
            if not model_name:
                await cursor.execute(
                    """
                    SELECT model_name
                    FROM user_mining_models
                    WHERE mining_function = 'EMBEDDING'
                    ORDER BY model_name
                    FETCH FIRST 1 ROW ONLY
                    """
                )
                row = await cursor.fetchone()
                if row is None:
                    raise RuntimeError("No Oracle embedding model is installed")
                model_name = str(row[0])

            await cursor.execute(
                f"SELECT VECTOR_EMBEDDING({_quoted_identifier(model_name)} "
                "USING :text AS DATA) FROM dual",
                {"text": "embedding dimension check"},
            )
            row = await cursor.fetchone()
            if row is None or row[0] is None:
                raise RuntimeError("Oracle returned no embedding during model validation")
            dimension = len(row[0])
    return model_name, dimension


async def initialize_model() -> None:
    global _loaded_dimension, _model_name, _source_shard

    if settings.embedding_provider != "oracle":
        raise RuntimeError(f"Unsupported embedding provider: {settings.embedding_provider}")

    errors: list[str] = []
    for shard_id in _candidate_shards():
        if not registry.reserve(shard_id):
            continue
        try:
            model_name, dimension = await _discover_model(shard_id)
            if dimension != settings.embedding_dimension:
                raise RuntimeError(
                    f"Oracle embedding model dimension {dimension} does not match "
                    f"configured dimension {settings.embedding_dimension}"
                )
            registry.circuit_breaker.record_success(shard_id)
            _model_name = model_name
            _source_shard = shard_id
            _loaded_dimension = dimension
            logger.info(
                "Using Oracle embedding model %s on shard %s",
                model_name,
                shard_id,
            )
            return
        except Exception as exc:
            registry.circuit_breaker.record_failure(shard_id)
            errors.append(f"{shard_id}: {type(exc).__name__}")
            logger.exception("Could not initialize Oracle embeddings on %s", shard_id)

    detail = ", ".join(errors) if errors else "no shard was available"
    raise RuntimeError(f"No Oracle embedding model could be initialized ({detail})")


async def _embed_on_shard(shard_id: str, texts: list[str]) -> list[list[float]]:
    if _model_name is None:
        raise RuntimeError("Oracle embedding model is not initialized")
    pool = get_pool(shard_id)
    if pool is None:
        raise RuntimeError(f"No connection pool is available for shard {shard_id}")

    query = (
        f"SELECT VECTOR_EMBEDDING({_quoted_identifier(_model_name)} USING :text AS DATA) FROM dual"
    )
    embeddings: list[list[float]] = []
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            for text in texts:
                await cursor.execute(query, {"text": text})
                row = await cursor.fetchone()
                if row is None or row[0] is None:
                    raise RuntimeError("Oracle returned no embedding")
                embedding = list(row[0])
                if len(embedding) != settings.embedding_dimension:
                    raise RuntimeError(
                        f"Oracle returned embedding dimension {len(embedding)}; "
                        f"expected {settings.embedding_dimension}"
                    )
                embeddings.append(embedding)
    return embeddings


async def embed_texts(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    if _model_name is None:
        raise RuntimeError("Oracle embedding model is not initialized")

    ordered = _candidate_shards()
    if _source_shard in ordered:
        ordered.remove(_source_shard)
        ordered.insert(0, _source_shard)

    for shard_id in ordered:
        if not registry.reserve(shard_id):
            continue
        try:
            embeddings = await _embed_on_shard(shard_id, texts)
            registry.circuit_breaker.record_success(shard_id)
            return embeddings
        except Exception:
            registry.circuit_breaker.record_failure(shard_id)
            logger.exception("Oracle embedding failed on shard %s", shard_id)
    raise RuntimeError("No Oracle shard could generate embeddings")


async def embed_text(text: str) -> list[float]:
    return (await embed_texts([text]))[0]


def model_info() -> dict[str, Any]:
    return {
        "provider": settings.embedding_provider,
        "model": _model_name or settings.oracle_embedding_model,
        "configured_dimension": settings.embedding_dimension,
        "loaded_dimension": _loaded_dimension,
        "source_shard": _source_shard,
        "initialized": _model_name is not None,
        "hosted_in_gateway": False,
    }
