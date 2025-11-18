from dataclasses import dataclass
from datetime import datetime
from typing import Dict
from ..clients.base import BaseAirClient

@dataclass
class AirPollutionInfo:
    location: str
    aqi: int
    components: Dict[str, float]  # np. {"pm2_5": 12.3, "no2": 18.4, ...}
    updated_at: datetime
    data_provider: str

class AirPollutionRepository:
    def __init__(self, air_client: BaseAirClient):
        self.client = air_client
    
    async def get_current_air_quality(self, lat: float, lon: float) -> AirPollutionInfo:
        data = await self.client.get_air_quality(lat, lon)
        
        return AirPollutionInfo(
            location=f"{lat},{lon}",
            aqi=data.get("aqi", 0),
            components=data.get("components", {}),
            updated_at=datetime.now(),
            data_provider=data.get("provider", "unknown")
        )
