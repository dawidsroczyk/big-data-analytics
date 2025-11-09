from dataclasses import dataclass
from datetime import datetime
from typing import List
from ..clients.base import BaseWeatherClient

@dataclass
class WeatherInfo:
    location: str
    temperature: float
    conditions: str
    humidity: float
    wind_speed: float
    updated_at: datetime
    data_provider: str

class WeatherRepository:
    def __init__(self, weather_client: BaseWeatherClient):
        self.client = weather_client
    
    async def get_current_conditions(self, lat: float, lng: float) -> WeatherInfo:
        data = await self.client.get_current_weather(lat, lng)
        
        return WeatherInfo(
            location=f"{lat},{lng}",
            temperature=data.get('temperature', 0),
            conditions=data.get('conditions', 'unknown'),
            humidity=data.get('humidity', 0),
            wind_speed=data.get('wind_speed', 0),
            updated_at=datetime.now(),
            data_provider=data.get('provider', 'unknown')
        )
