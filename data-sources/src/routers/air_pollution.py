from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from ..container import container
from datetime import datetime

router = APIRouter()

class AirPollutionResponse(BaseModel):
    location: str
    aqi: int
    co: float
    no2: float
    o3: float
    pm2_5: float
    pm10: float
    so2: float
    updated_at: datetime
    data_provider: str

@router.get("/air_pollution", response_model=AirPollutionResponse)
async def get_air_pollution(lat: float = Query(...), lng: float = Query(...)):
    try:
        repo = container.air_pollution_repository()
        # The repository layer expects `lon`, so we pass `lng` as `lon`.
        pollution_info = await repo.get_current_air_pollution(lat, lon=lng)
        return AirPollutionResponse(
            location=pollution_info.location,
            aqi=pollution_info.aqi,
            co=pollution_info.co,
            no2=pollution_info.no2,
            o3=pollution_info.o3,
            pm2_5=pollution_info.pm2_5,
            pm10=pollution_info.pm10,
            so2=pollution_info.so2,
            updated_at=pollution_info.updated_at,
            data_provider=pollution_info.data_provider
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
