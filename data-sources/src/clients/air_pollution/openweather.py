import httpx
from datetime import datetime
from ..base import BaseAirClient

class OpenWeatherAirPollutionClient(BaseAirClient):
    """Air pollution client using OpenWeather API"""

    async def get_air_quality(self, lat: float, lon: float):
        url = f"{self.base_url}/air_pollution"

        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

        entry = data["list"][0]

        return {
            "provider": "openweather",
            "location": f"{lat},{lon}",
            "timestamp": datetime.now().isoformat(),
            "aqi": entry["main"]["aqi"],
            "components": entry["components"],
            "raw": data
        }
