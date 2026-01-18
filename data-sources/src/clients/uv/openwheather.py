from typing import Dict, Any
import httpx
from ..base import BaseUVClient, ClientConfig
from datetime import datetime, timezone

class OpenWeatherUVClient(BaseUVClient):
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

        # Extract values with fallbacks to match Mock format (int for dt, str for timestamp)
        uv_value = data.get("value")
        dt = data.get("date") or data.get("dt")
        
        if data.get("date_iso"):
            timestamp = data.get("date_iso")
        elif dt:
            timestamp = datetime.fromtimestamp(dt, timezone.utc).isoformat()
        else:
            now = datetime.now(timezone.utc)
            timestamp = now.isoformat()
            dt = int(now.timestamp())
        now = datetime.utcnow()
        return {
            "uv_index": uv_value,
            "location": f"{lat},{lng}",
            # "timestamp": timestamp,
            "timestamp": now.isoformat(),
            # "dt": dt,
            "dt": int(now.timestamp()),
            "provider": "openweathermap"
        }