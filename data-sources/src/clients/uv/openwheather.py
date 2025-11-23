from typing import Dict, Any
import httpx
from ..base import BaseWeatherClient, ClientConfig
from datetime import datetime


class OpenWeatherUVClient(BaseWeatherClient):
    """OpenWeatherMap UV client (minimal, uses httpx)."""

    def __init__(self, config: ClientConfig):
        super().__init__(config)

    async def get_uv_index(self, lat: float, lng: float) -> Dict[str, Any]:
        url = f"{self.config.base_url.rstrip('/')}/uvi"
        params = {
            "lat": lat,
            "lon": lng,
            "appid": self.config.api_key
        }

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        # OWM UVI response historically contains 'value' and 'date_iso' (legacy)
        uv_value = data.get("value")
        timestamp = data.get("date_iso") or data.get("date") or datetime.utcnow().isoformat()
        dt = data.get("date") or data.get("dt")

        return {
            "uv_index": uv_value,
            "location": f"{lat},{lng}",
            "timestamp": timestamp,
            "dt": dt,
            "provider": "openweathermap"
        }
