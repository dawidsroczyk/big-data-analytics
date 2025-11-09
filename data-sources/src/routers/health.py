from fastapi import APIRouter
from ..container import container

router = APIRouter()

@router.get("/health")
async def health_check():
    config = container.config()
    return {
        "status": "healthy",
        "data_sources": {
            "traffic": config.traffic.provider,
            "weather": config.weather.provider
        }
    }