# repositories/air_pollutions.py
from ..clients.air_pollutions import create_air_pollution_client
from ..clients.air_pollutions.mock import MockAirPollutionClient
from datetime import datetime
from typing import Any

class AirPollutionRepository:
    """Repository that expects a client instance (same pattern as traffic/weather repos)."""
    def __init__(self, air_pollution_client):
        # client should implement get_current_air_pollution(lat, lon)
        self.client = air_pollution_client

    async def get_current_air_pollution(self, lat: float, lon: float):
        # Returns a simple object with attributes similar to weather_info
        data = await self.client.get_current_air_pollution(lat, lon)
        class Result:
            def __init__(self, data):
                self.location = data["location"]
                self.aqi = data["aqi"]
                self.co = data["co"]
                self.no2 = data["no2"]
                self.o3 = data["o3"]
                self.pm2_5 = data["pm2_5"]
                self.pm10 = data["pm10"]
                self.so2 = data["so2"]
                self.updated_at = datetime.fromisoformat(data["timestamp"])
                self.data_provider = data["provider"]
        return Result(data)
