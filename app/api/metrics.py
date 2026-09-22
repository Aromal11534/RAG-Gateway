from fastapi import APIRouter, Response

from app.observability.metrics import metrics

router = APIRouter(tags=["observability"])


@router.get("/metrics", include_in_schema=False)
async def get_metrics():
    return Response(
        content=metrics.render(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
