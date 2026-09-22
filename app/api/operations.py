from fastapi import APIRouter, HTTPException

from app.jobs.manager import job_manager

router = APIRouter(prefix="/operations", tags=["operations"])


@router.get("/{operation_id}")
async def get_operation(operation_id: str):
    operation = job_manager.get(operation_id)
    if operation is None:
        raise HTTPException(status_code=404, detail="Operation not found")
    return operation
