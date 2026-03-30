from fastapi import APIRouter

from proteinvault.api.v1.datasets import router as datasets_router
from proteinvault.api.v1.models import router as models_router

v1_router = APIRouter()
v1_router.include_router(datasets_router)
v1_router.include_router(models_router)


@v1_router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
