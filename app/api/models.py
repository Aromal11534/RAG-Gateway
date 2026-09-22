from fastapi import APIRouter

from app.embeddings.adapter import model_info

router = APIRouter(prefix="/models", tags=["models"])


@router.get("/embedding")
async def get_embedding_model():
    return model_info()
