import array
import inspect
import json
from typing import Any, Dict, List, Optional

from app.database.pools import get_pool

VECTOR_SEARCH_QUERY = """
SELECT id,
       VECTOR_DISTANCE(embedding, :embedding, COSINE) AS score,
       chunk_text,
       metadata_json,
       revision,
       content_hash,
       document_id,
       chunk_index
FROM vector_items
WHERE namespace = :namespace
ORDER BY score
FETCH FIRST :top_k ROWS ONLY
"""

INSERT_QUERY = """
INSERT INTO vector_items
    (id, namespace, chunk_text, metadata_json, embedding, revision, content_hash,
     document_id, chunk_index)
VALUES
    (:id, :namespace, :chunk_text, :metadata_json, :embedding, :revision, :content_hash,
     :document_id, :chunk_index)
"""

UPSERT_QUERY = """
MERGE INTO vector_items target
USING (SELECT :id AS id, :namespace AS namespace FROM dual) source
ON (target.id = source.id AND target.namespace = source.namespace)
WHEN MATCHED THEN UPDATE SET
    target.chunk_text = :chunk_text,
    target.metadata_json = :metadata_json,
    target.embedding = :embedding,
    target.revision = :revision,
    target.content_hash = :content_hash,
    target.document_id = :document_id,
    target.chunk_index = :chunk_index,
    target.updated_at = SYSTIMESTAMP
    WHERE :revision >= target.revision
WHEN NOT MATCHED THEN INSERT
    (id, namespace, chunk_text, metadata_json, embedding, revision, content_hash,
     document_id, chunk_index, created_at, updated_at)
VALUES
    (:id, :namespace, :chunk_text, :metadata_json, :embedding, :revision, :content_hash,
     :document_id, :chunk_index, SYSTIMESTAMP, SYSTIMESTAMP)
"""

GET_QUERY = """
SELECT id, namespace, chunk_text, metadata_json, embedding, revision, content_hash,
       document_id, chunk_index
FROM vector_items
WHERE id = :id AND namespace = :namespace
FETCH FIRST 1 ROW ONLY
"""

GET_DOCUMENT_QUERY = """
SELECT id, namespace, chunk_text, metadata_json, revision, content_hash,
       document_id, chunk_index
FROM vector_items
WHERE namespace = :namespace AND document_id = :document_id
ORDER BY chunk_index, id
"""

DELETE_QUERY = "DELETE FROM vector_items WHERE id = :id AND namespace = :namespace"
DELETE_DOCUMENT_QUERY = (
    "DELETE FROM vector_items WHERE namespace = :namespace AND document_id = :document_id"
)
DELETE_NAMESPACE_QUERY = "DELETE FROM vector_items WHERE namespace = :namespace"
CLEAR_QUERY = "DELETE FROM vector_items"

SCAN_VECTORS_QUERY = """
SELECT id, namespace, chunk_text, metadata_json, embedding, revision, content_hash,
       document_id, chunk_index
FROM vector_items
WHERE (:namespace IS NULL OR namespace = :namespace)
  AND (
      :after_namespace IS NULL
      OR namespace > :after_namespace
      OR (namespace = :after_namespace AND id > :after_id)
  )
ORDER BY namespace, id
FETCH FIRST :page_size ROWS ONLY
"""


def _require_pool(shard_id: str):
    pool = get_pool(shard_id)
    if pool is None:
        raise RuntimeError(f"No connection pool is available for shard {shard_id}")
    return pool


def _bind_values(
    item_id: str,
    namespace: str,
    text: str,
    metadata: dict,
    embedding: List[float],
    revision: int = 0,
    content_hash: str | None = None,
    document_id: str | None = None,
    chunk_index: int | None = None,
) -> dict:
    return {
        "id": item_id,
        "namespace": namespace,
        "chunk_text": text,
        "metadata_json": json.dumps(
            metadata,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "embedding": array.array("f", embedding),
        "revision": revision,
        "content_hash": content_hash,
        "document_id": document_id,
        "chunk_index": chunk_index,
    }


async def _read_lob(value: Any) -> Any:
    if isinstance(value, dict):
        return value
    if hasattr(value, "read"):
        value = value.read()
        if inspect.isawaitable(value):
            value = await value
    return value


async def _decode_text(value: Any) -> str:
    value = await _read_lob(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    return value if isinstance(value, str) else str(value)


async def _decode_metadata(value: Any) -> dict:
    if value is None:
        return {}
    value = await _read_lob(value)
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    decoded = json.loads(value)
    return decoded if isinstance(decoded, dict) else {"value": decoded}


async def insert_vector(
    shard_id: str,
    item_id: str,
    namespace: str,
    text: str,
    metadata: dict,
    embedding: List[float],
    revision: int = 0,
    content_hash: str | None = None,
    document_id: str | None = None,
    chunk_index: int | None = None,
) -> None:
    pool = _require_pool(shard_id)
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                INSERT_QUERY,
                _bind_values(
                    item_id,
                    namespace,
                    text,
                    metadata,
                    embedding,
                    revision,
                    content_hash,
                    document_id,
                    chunk_index,
                ),
            )
        await connection.commit()


async def upsert_vector(
    shard_id: str,
    item_id: str,
    namespace: str,
    text: str,
    metadata: dict,
    embedding: List[float],
    revision: int = 0,
    content_hash: str | None = None,
    document_id: str | None = None,
    chunk_index: int | None = None,
) -> None:
    pool = _require_pool(shard_id)
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                UPSERT_QUERY,
                _bind_values(
                    item_id,
                    namespace,
                    text,
                    metadata,
                    embedding,
                    revision,
                    content_hash,
                    document_id,
                    chunk_index,
                ),
            )
        await connection.commit()


async def search_shard(
    shard_id: str,
    namespace: str,
    embedding: List[float],
    top_k: int,
) -> List[Dict[str, Any]]:
    pool = _require_pool(shard_id)
    results: List[Dict[str, Any]] = []
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                VECTOR_SEARCH_QUERY,
                {
                    "namespace": namespace,
                    "embedding": array.array("f", embedding),
                    "top_k": top_k,
                },
            )
            rows = await cursor.fetchall()
            for row in rows:
                results.append(
                    {
                        "id": row[0],
                        "score": float(row[1]),
                        "text": await _decode_text(row[2]),
                        "metadata": await _decode_metadata(row[3]),
                        "revision": int(row[4]) if len(row) > 4 and row[4] else 0,
                        "content_hash": row[5] if len(row) > 5 else None,
                        "document_id": row[6] if len(row) > 6 else None,
                        "chunk_index": row[7] if len(row) > 7 else None,
                        "shard_id": shard_id,
                    }
                )
    return results


async def get_vector(
    shard_id: str,
    item_id: str,
    namespace: str,
) -> Optional[Dict[str, Any]]:
    pool = _require_pool(shard_id)
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(GET_QUERY, {"id": item_id, "namespace": namespace})
            row = await cursor.fetchone()
            if row is None:
                return None
            return {
                "id": row[0],
                "namespace": row[1],
                "text": await _decode_text(row[2]),
                "metadata": await _decode_metadata(row[3]),
                "_embedding": list(row[4]) if row[4] is not None else [],
                "revision": int(row[5]) if row[5] else 0,
                "content_hash": row[6],
                "document_id": row[7],
                "chunk_index": row[8],
                "shard_id": shard_id,
            }


async def get_document_chunks(
    shard_id: str,
    namespace: str,
    document_id: str,
) -> List[Dict[str, Any]]:
    pool = _require_pool(shard_id)
    results: List[Dict[str, Any]] = []
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                GET_DOCUMENT_QUERY,
                {"namespace": namespace, "document_id": document_id},
            )
            for row in await cursor.fetchall():
                results.append(
                    {
                        "id": row[0],
                        "namespace": row[1],
                        "text": await _decode_text(row[2]),
                        "metadata": await _decode_metadata(row[3]),
                        "revision": int(row[4]) if row[4] else 0,
                        "content_hash": row[5],
                        "document_id": row[6],
                        "chunk_index": row[7],
                        "shard_id": shard_id,
                    }
                )
    return results


async def scan_vector_page(
    shard_id: str,
    *,
    namespace: str | None,
    after_namespace: str | None,
    after_id: str | None,
    page_size: int,
) -> List[Dict[str, Any]]:
    pool = _require_pool(shard_id)
    results: List[Dict[str, Any]] = []
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                SCAN_VECTORS_QUERY,
                {
                    "namespace": namespace,
                    "after_namespace": after_namespace,
                    "after_id": after_id,
                    "page_size": page_size,
                },
            )
            for row in await cursor.fetchall():
                results.append(
                    {
                        "id": row[0],
                        "namespace": row[1],
                        "text": await _decode_text(row[2]),
                        "metadata": await _decode_metadata(row[3]),
                        "_embedding": list(row[4]) if row[4] is not None else [],
                        "revision": int(row[5]) if row[5] else 0,
                        "content_hash": row[6],
                        "document_id": row[7],
                        "chunk_index": row[8],
                        "shard_id": shard_id,
                    }
                )
    return results


async def delete_vector(shard_id: str, item_id: str, namespace: str) -> int:
    return await _execute_delete(
        shard_id,
        DELETE_QUERY,
        {"id": item_id, "namespace": namespace},
    )


async def delete_document(shard_id: str, document_id: str, namespace: str) -> int:
    return await _execute_delete(
        shard_id,
        DELETE_DOCUMENT_QUERY,
        {"document_id": document_id, "namespace": namespace},
    )


async def delete_namespace(shard_id: str, namespace: str) -> int:
    return await _execute_delete(
        shard_id,
        DELETE_NAMESPACE_QUERY,
        {"namespace": namespace},
    )


async def clear_vectors(shard_id: str) -> int:
    return await _execute_delete(shard_id, CLEAR_QUERY, {})


async def _execute_delete(shard_id: str, query: str, binds: dict) -> int:
    pool = _require_pool(shard_id)
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(query, binds)
            affected = cursor.rowcount
        await connection.commit()
    return max(0, affected)
