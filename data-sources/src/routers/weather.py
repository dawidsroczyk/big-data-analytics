from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..container import container

router = APIRouter()

class WeatherResponse(BaseModel):
    location: str
    temperature: float
    conditions: str
    humidity: float
    wind_speed: float
    updated_at: str
    data_provider: str

@router.get("/weather", response_model=WeatherResponse)
async def get_weather(lat: float, lng: float):
    try:
        repo = container.weather_repository()
        weather_info = await repo.get_current_conditions(lat, lng)
        return WeatherResponse(
            location=weather_info.location,
            temperature=weather_info.temperature,
            conditions=weather_info.conditions,
            humidity=weather_info.humidity,
            wind_speed=weather_info.wind_speed,
            updated_at=weather_info.updated_at.isoformat(),
            data_provider=weather_info.data_provider
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))