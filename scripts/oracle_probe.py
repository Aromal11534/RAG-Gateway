"""Read-only Oracle AI Database and vector capability probe."""

import argparse
import json
from typing import Any

import oracledb

from app.config import settings


def _oracle_error(exc: oracledb.Error) -> dict[str, Any]:
    error = exc.args[0] if exc.args else exc
    return {
        "code": getattr(error, "code", None),
        "message": str(getattr(error, "message", error)).strip(),
    }


def _quoted_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def run_probe(shard_number: int) -> tuple[dict[str, Any], int]:
    shard_id = f"oracle_{shard_number:02d}"
    config = settings.get_shards().get(shard_id)
    if config is None:
        return {
            "status": "configuration_error",
            "shard": shard_id,
            "message": "The selected DB user/password/DSN triplet is incomplete.",
        }, 2

    result: dict[str, Any] = {"status": "checking", "shard": shard_id}
    try:
        with oracledb.connect(
            user=config.user,
            password=config.password.get_secret_value(),
            dsn=config.dsn,
            tcp_connect_timeout=5,
        ) as connection:
            result["database_version"] = connection.version
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER'),
                           SYS_CONTEXT('USERENV', 'DB_NAME')
                    FROM dual
                    """
                )
                current_user, database_name = cursor.fetchone()
                result["current_user"] = current_user
                result["database_name"] = database_name

                try:
                    cursor.execute(
                        """
                        SELECT VECTOR_DISTANCE(
                            TO_VECTOR('[1,0]'),
                            TO_VECTOR('[0,1]'),
                            COSINE
                        )
                        FROM dual
                        """
                    )
                    result["vector_sql"] = {
                        "supported": True,
                        "test_distance": float(cursor.fetchone()[0]),
                    }
                except oracledb.Error as exc:
                    result["vector_sql"] = {
                        "supported": False,
                        "error": _oracle_error(exc),
                    }

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM user_tables
                    WHERE table_name = 'VECTOR_ITEMS'
                    """
                )
                table_exists = bool(cursor.fetchone()[0])
                result["vector_items_table"] = {"exists": table_exists}
                if table_exists:
                    cursor.execute(
                        """
                        SELECT column_name, data_type
                        FROM user_tab_columns
                        WHERE table_name = 'VECTOR_ITEMS'
                        ORDER BY column_id
                        """
                    )
                    columns = {name: data_type for name, data_type in cursor.fetchall()}
                    required_columns = {
                        "NAMESPACE",
                        "ID",
                        "CHUNK_TEXT",
                        "METADATA_JSON",
                        "EMBEDDING",
                        "REVISION",
                        "CONTENT_HASH",
                        "DOCUMENT_ID",
                        "CHUNK_INDEX",
                    }
                    result["vector_items_table"]["embedding_type"] = columns.get("EMBEDDING")
                    result["vector_items_table"]["missing_columns"] = sorted(
                        required_columns - columns.keys()
                    )
                    cursor.execute(
                        """
                        SELECT index_name, status
                        FROM user_indexes
                        WHERE table_name = 'VECTOR_ITEMS'
                        ORDER BY index_name
                        """
                    )
                    result["vector_items_table"]["indexes"] = [
                        {"name": name, "status": status} for name, status in cursor.fetchall()
                    ]

                try:
                    cursor.execute(
                        """
                        SELECT model_name, algorithm
                        FROM user_mining_models
                        WHERE mining_function = 'EMBEDDING'
                        ORDER BY model_name
                        """
                    )
                    models = [
                        {"name": name, "algorithm": algorithm}
                        for name, algorithm in cursor.fetchall()
                    ]
                    result["oracle_embedding"] = {
                        "catalog_accessible": True,
                        "models": models,
                    }
                    if models:
                        model_name = models[0]["name"]
                        cursor.execute(
                            f"SELECT VECTOR_EMBEDDING({_quoted_identifier(model_name)} "
                            "USING :text AS DATA) FROM dual",
                            {"text": "Oracle embedding smoke test"},
                        )
                        result["oracle_embedding"]["test_model"] = model_name
                        embedding = cursor.fetchone()[0]
                        result["oracle_embedding"]["test_dimension"] = len(embedding)
                except oracledb.Error as exc:
                    result["oracle_embedding"] = {
                        "catalog_accessible": False,
                        "error": _oracle_error(exc),
                    }

        vector_supported = result.get("vector_sql", {}).get("supported", False)
        table_status = result.get("vector_items_table", {})
        schema_ready = table_status.get("exists", False) and not table_status.get(
            "missing_columns", []
        )
        result["status"] = "ready" if vector_supported and schema_ready else "not_ready"
        return result, 0 if result["status"] == "ready" else 1
    except oracledb.Error as exc:
        result["status"] = "connection_failed"
        result["error"] = _oracle_error(exc)
        return result, 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, choices=range(1, 7), default=4)
    args = parser.parse_args()
    result, exit_code = run_probe(args.shard)
    print(json.dumps(result, indent=2, default=str))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
