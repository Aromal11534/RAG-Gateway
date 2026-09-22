import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import admin, documents, health, metrics, models, operations, search, vectors
from app.config import settings
from app.database.pools import close_pools, initialize_pools
from app.embeddings.adapter import initialize_model
from app.jobs.manager import job_manager
from app.middleware import RequestBodyLimitMiddleware, operational_middleware
from app.security import authentication_middleware

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Check database credentials before a first-run model download. This gives
    # fast, actionable startup failures when Oracle configuration is invalid.
    await initialize_pools()
    try:
        await initialize_model()
        yield
    finally:
        await job_manager.shutdown()
        await close_pools()


app = FastAPI(
    title="Federated VectorDB Gateway",
    description=(
        "Microservice for abstracting Oracle Autonomous AI Database instances "
        "as a single logical VectorDB."
    ),
    version="1.1.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.docs_enabled else None,
    redoc_url="/redoc" if settings.docs_enabled else None,
    openapi_url="/openapi.json" if settings.docs_enabled else None,
)

app.middleware("http")(authentication_middleware)
app.middleware("http")(operational_middleware)
app.add_middleware(RequestBodyLimitMiddleware)
app.include_router(vectors.router)
app.include_router(documents.router)
app.include_router(search.router)
app.include_router(admin.router)
app.include_router(health.router)
app.include_router(operations.router)
app.include_router(metrics.router)
app.include_router(models.router)


@app.get("/")
async def root():
    return {"message": "Federated VectorDB Gateway is running"}
