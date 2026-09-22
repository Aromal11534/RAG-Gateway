# Federated VectorDB Gateway

A FastAPI gateway that presents up to six Oracle AI Database instances as one logical
vector store. Writes are assigned with weighted consistent hashing. Searches fan out to
available shards, merge comparable cosine distances, and clearly identify partial results.

The gateway also provides version-aware replication, read repair, online rebalancing,
batch vector APIs, document chunk ingestion, metadata filters, hybrid lexical reranking,
background operation tracking, request limits, maintenance draining, and Prometheus-format
metrics.

## Security model

- `/`, `/live`, `/ready`, and `/health` are public so probes can run.
- Every other route requires `X-API-Key`.
- `/admin/*` additionally requires a distinct `X-Admin-API-Key`.
- API documentation is disabled by default. Set `DOCS_ENABLED=true` only in a controlled
  environment.
- `namespace` is mandatory for every data read or mutation and is bound in Oracle SQL.
  The gateway key is a trusted service credential; it is not a per-tenant identity system.

Never commit `.env`. Use a managed secret store in production.

## Setup

1. Create the table on every shard with `schema/001_create_vector_items.sql`.
2. Copy `.env.example` to `.env`, generate independent gateway/admin keys of at least 32
   characters, and configure
   at least one complete `DBn_USER`, `DBn_PASSWORD`, and `DBn_DSN` triplet.
3. Load an ONNX embedding model into Oracle so it appears in `USER_MINING_MODELS` with
   `MINING_FUNCTION = 'EMBEDDING'`. Set `ORACLE_EMBEDDING_MODEL` to its exact catalog name,
   or leave it blank to auto-discover the first available embedding model.
4. Keep `EMBEDDING_DIMENSION` equal to both the Oracle model output and table VECTOR
   dimension. The current schema uses 384 dimensions.
5. Run `docker compose up --build`.

For an existing installation created before version 1.1, apply
`schema/002_add_consistency_and_documents.sql` to every shard before starting the upgraded
gateway. New installations only need `schema/001_create_vector_items.sql`.

If a secret contains `$`, prefer a separate `.gateway.env` file and start Compose with
`GATEWAY_ENV_FILE=.gateway.env`. This prevents Compose from interpreting secret fragments
while loading its automatic `.env` interpolation file.

Run a read-only database and vector-capability check for the configured fourth
shard with:

```text
docker compose run --rm gateway python scripts/oracle_probe.py --shard 4
```

The probe does not insert, update, or delete data. Its `oracle_embedding.models`
list shows whether an ONNX embedding model has been loaded into Oracle itself.

Compose binds to `127.0.0.1:8000` by default. Set `GATEWAY_PORT` to change the host port;
use an explicit reverse proxy or Compose network rather than exposing Uvicorn directly.

## API

All examples below also require `X-API-Key: <gateway key>`.

- `POST /vectors` inserts a new vector; duplicate `(namespace, id)` values return `409`.
- `PUT /vectors/{id}` performs an Oracle `MERGE` upsert.
- `POST /vectors/batch` embeds and writes multiple vectors in one request and reports
  per-item failures with HTTP `207`.
- `GET /vectors/{id}?namespace=...` reads across possible fallback placements.
- `DELETE /vectors/{id}?namespace=...` deletes all fallback copies.
- `POST /search` accepts `namespace`, `query`, and `top_k`.
- `POST /documents` chunks and embeds a document. Add `?background=true` to receive an
  operation ID immediately.
- `GET/DELETE /documents/{id}?namespace=...` reads or removes all document chunks.
- `GET /operations/{id}` reports background ingestion and rebalance state.
- `GET /models/embedding` reports the configured and loaded embedding model.
- `DELETE /admin/namespace/{namespace}` deletes one namespace on every shard.
- `POST /admin/reinitialize` requires body
  `{"confirm":"DELETE_ALL_VECTOR_DATA"}` and deletes every vector on every shard.
- `GET /health` performs database pings and returns `503` when no shard is usable.
- `GET /live` is a process liveness probe; `GET /ready` performs database readiness checks.
- `GET /stats` reports pool and circuit-breaker state without credentials or DSNs.
- `GET /metrics` returns Prometheus text metrics and requires the gateway API key.
- `GET /admin/shards` reports shard state; shard drain/activate routes provide temporary
  maintenance control.
- `POST /admin/placement` previews placement for an ID.
- `POST /admin/rebalance` starts an online placement-repair job. `delete_extras` defaults to
  false; enable it only after validating the generated replicas.

Search can return `partial: true` when at least one shard succeeds and another is
unavailable. It returns `503` if no shard completes the search. Destructive operations
require every shard to be available; a partially completed idempotent deletion returns
`503` and can be safely retried.

Writes carry a process-monotonic revision and the same revision is written to every replica.
Reads and searches prefer the newest revision rather than the nearest stale copy. Point reads
schedule repair of missing or stale intended replicas. For deployments with multiple gateway
processes on hosts whose clocks may differ, use a single writer or replace the built-in
revision generator with a shared logical-clock service.

Search supports optional exact metadata filters, `max_distance`, and `ranking="hybrid"`.
Hybrid mode reranks an oversampled vector candidate set using lexical term overlap; it is not
a replacement for a full Oracle Text index when lexical-only recall is required.

Background jobs and maintenance drain state are currently process-local and intentionally
report `durable: false`. Use one gateway worker for these operations, or replace the job
manager with a durable queue before horizontally scaling administrative workloads.

## Local development

Install `requirements-dev.txt`, then run:

```text
ruff check .
pytest
uvicorn app.main:app --reload
```

No embedding model is downloaded or hosted by the gateway. During startup it discovers or
validates the Oracle model with `VECTOR_EMBEDDING`, then sends text to Oracle for all query,
vector, batch, and document embeddings. Local development therefore requires a reachable
configured Oracle shard with the embedding model installed.
