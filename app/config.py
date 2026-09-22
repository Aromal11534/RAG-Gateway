from typing import Dict, Literal, Optional

from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ShardConfig(BaseModel):
    user: str
    password: SecretStr
    dsn: str
    weight: float = Field(default=1.0, gt=0)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", ".gateway.env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Gateway and API security. Missing keys fail closed in the request boundary.
    gateway_api_key: Optional[SecretStr] = None
    admin_api_key: Optional[SecretStr] = None
    docs_enabled: bool = False
    log_level: str = "INFO"

    # Embedding and request limits.
    embedding_provider: Literal["oracle"] = "oracle"
    oracle_embedding_model: Optional[str] = None
    oracle_embedding_shard: Optional[str] = Field(
        default=None,
        pattern=r"^oracle_0[1-6]$",
    )
    embedding_dimension: int = Field(default=384, gt=0)
    max_top_k: int = Field(default=100, ge=1, le=1000)
    max_text_length: int = Field(default=100_000, ge=1)
    max_metadata_bytes: int = Field(default=65_536, ge=2)
    max_request_body_bytes: int = Field(default=2_000_000, ge=1_024)
    max_batch_size: int = Field(default=100, ge=1, le=1_000)
    max_batch_concurrency: int = Field(default=8, ge=1, le=64)
    default_chunk_size: int = Field(default=1_200, ge=100, le=100_000)
    default_chunk_overlap: int = Field(default=200, ge=0)
    max_document_chunks: int = Field(default=1_000, ge=1, le=10_000)
    search_candidate_multiplier: int = Field(default=3, ge=1, le=20)

    # Placement and write durability. Values are clamped to the number of
    # configured shards at runtime, so the defaults also work in single-shard
    # development environments.
    replication_factor: int = Field(default=2, ge=1, le=6)
    write_quorum: int = Field(default=1, ge=1, le=6)

    # In-process protection. Set to zero to disable rate limiting.
    rate_limit_requests_per_minute: int = Field(default=0, ge=0)

    # Database resilience.
    db_pool_min: int = Field(default=1, ge=0)
    db_pool_max: int = Field(default=10, ge=1)
    db_pool_increment: int = Field(default=1, ge=1)
    db_pool_wait_timeout_ms: int = Field(default=5_000, ge=1)
    shard_query_timeout_seconds: float = Field(default=10.0, gt=0)
    circuit_failure_threshold: int = Field(default=3, ge=1)
    circuit_recovery_seconds: float = Field(default=30.0, gt=0)

    # Oracle shards.
    db1_user: Optional[str] = None
    db1_password: Optional[SecretStr] = None
    db1_dsn: Optional[str] = None
    db1_weight: float = Field(default=1.0, gt=0)

    db2_user: Optional[str] = None
    db2_password: Optional[SecretStr] = None
    db2_dsn: Optional[str] = None
    db2_weight: float = Field(default=1.0, gt=0)

    db3_user: Optional[str] = None
    db3_password: Optional[SecretStr] = None
    db3_dsn: Optional[str] = None
    db3_weight: float = Field(default=1.0, gt=0)

    db4_user: Optional[str] = None
    db4_password: Optional[SecretStr] = None
    db4_dsn: Optional[str] = None
    db4_weight: float = Field(default=1.0, gt=0)

    db5_user: Optional[str] = None
    db5_password: Optional[SecretStr] = None
    db5_dsn: Optional[str] = None
    db5_weight: float = Field(default=1.0, gt=0)

    db6_user: Optional[str] = None
    db6_password: Optional[SecretStr] = None
    db6_dsn: Optional[str] = None
    db6_weight: float = Field(default=1.0, gt=0)

    @model_validator(mode="after")
    def validate_related_settings(self):
        if self.db_pool_min > self.db_pool_max:
            raise ValueError("DB_POOL_MIN must not exceed DB_POOL_MAX")
        if self.write_quorum > self.replication_factor:
            raise ValueError("WRITE_QUORUM must not exceed REPLICATION_FACTOR")
        if self.default_chunk_overlap >= self.default_chunk_size:
            raise ValueError("DEFAULT_CHUNK_OVERLAP must be smaller than DEFAULT_CHUNK_SIZE")

        gateway_key = self.gateway_api_key.get_secret_value() if self.gateway_api_key else None
        admin_key = self.admin_api_key.get_secret_value() if self.admin_api_key else None
        if gateway_key and len(gateway_key) < 32:
            raise ValueError("GATEWAY_API_KEY must contain at least 32 characters")
        if admin_key and len(admin_key) < 32:
            raise ValueError("ADMIN_API_KEY must contain at least 32 characters")
        if gateway_key and admin_key and gateway_key == admin_key:
            raise ValueError("GATEWAY_API_KEY and ADMIN_API_KEY must be distinct")
        return self

    def get_shards(self) -> Dict[str, ShardConfig]:
        """Return only fully configured shards without exposing credentials."""
        shards: Dict[str, ShardConfig] = {}
        for i in range(1, 7):
            user = getattr(self, f"db{i}_user")
            password = getattr(self, f"db{i}_password")
            dsn = getattr(self, f"db{i}_dsn")
            weight = getattr(self, f"db{i}_weight")

            if user and password and dsn:
                shards[f"oracle_{i:02d}"] = ShardConfig(
                    user=user,
                    password=password,
                    dsn=dsn,
                    weight=weight,
                )

        return shards

    def get_incomplete_shards(self) -> Dict[str, list[str]]:
        """Return partially configured shards and their missing required fields."""
        incomplete: Dict[str, list[str]] = {}
        for i in range(1, 7):
            values = {
                "user": getattr(self, f"db{i}_user"),
                "password": getattr(self, f"db{i}_password"),
                "dsn": getattr(self, f"db{i}_dsn"),
            }
            if any(values.values()) and not all(values.values()):
                incomplete[f"oracle_{i:02d}"] = [
                    name for name, value in values.items() if not value
                ]
        return incomplete


settings = Settings()
