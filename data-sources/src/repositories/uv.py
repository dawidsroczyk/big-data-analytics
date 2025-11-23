from dataclasses import dataclass
from datetime import datetime
from typing import Any
from ..clients.base import BaseWeatherClient

@dataclass
class UVIndexInfo:
    uv_index: float
    location: str
    timestamp: datetime
    provider: str

class UVRepository:
    def __init__(self, uv_client: BaseWeatherClient):
        self.client = uv_client

    async def get_uv_index(self, lat: float, lng: float) -> UVIndexInfo:
        data = await self.client.get_uv_index(lat, lng)
        return UVIndexInfo(
            uv_index=data.get("uv_index", 0),
            location=data.get("location"),
            timestamp=datetime.fromisoformat(data.get("timestamp")),
            provider=data.get("provider", "unknown")
        )
