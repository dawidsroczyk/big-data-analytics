from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..container import container

router = APIRouter()


class AirQualityResponse(BaseModel):
    location: str
    aqi: int
    components: dict
    updated_at: str
    data_provider: str


@router.get("/air", response_model=AirQualityResponse)
async def get_air_quality(lat: float, lon: float):
    try:
        repo = container.air_pollution_repository()
        info = await repo.get_current_air_quality(lat, lon)
        return AirQualityResponse(
            location=info.location,
            aqi=info.aqi,
            components=info.components,
            updated_at=info.updated_at.isoformat(),
            data_provider=info.data_provider,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
