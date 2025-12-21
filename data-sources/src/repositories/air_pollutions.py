# repositories/air_pollutions.py
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from ..clients.base import BaseAirPollutionClient

@dataclass
class AirPollutionInfo:
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

class AirPollutionRepository:
    """Repository that expects a client instance (same pattern as traffic/weather repos)."""
    def __init__(self, air_pollution_client: BaseAirPollutionClient):
        # client should implement get_current_air_pollution(lat, lon)
        self.client = air_pollution_client

    async def get_current_air_pollution(self, lat: float, lon: float) -> AirPollutionInfo:
        # Returns a simple object with attributes similar to weather_info
        data = await self.client.get_current_air_pollution(lat, lon)
        
        return AirPollutionInfo(
            location=data.get("location", f"{lat},{lon}"),
            aqi=data.get("aqi"),
            co=data.get("co"),
            no2=data.get("no2"),
            o3=data.get("o3"),
            pm2_5=data.get("pm2_5"),
            pm10=data.get("pm10"),
            so2=data.get("so2"),
            updated_at=datetime.fromisoformat(data["timestamp"]),
            data_provider=data.get("provider", "unknown")
        )
