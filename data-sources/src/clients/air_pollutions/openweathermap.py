import httpx
from datetime import datetime, timezone
from typing import Dict, Any
from ..base import BaseAirPollutionClient, ClientConfig

class OpenWeatherMapAirPollutionClient(BaseAirPollutionClient):
    """
    Client for OpenWeatherMap Air Pollution API.
    It fetches pollution data.
    """

    def __init__(self, config: ClientConfig):
        super().__init__(config)

    async def get_current_air_pollution(self, lat: float, lon: float) -> Dict[str, Any]:
        """Fetches current air pollution data for given coordinates."""
        air_pollution_url = f"{self.config.base_url.rstrip('/')}/air_pollution"
        params = {"lat": lat, "lon": lon, "appid": self.config.api_key}

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            # Fetch pollution data
            pollution_resp = await client.get(air_pollution_url, params=params)
            pollution_resp.raise_for_status()
            pollution_data = pollution_resp.json()

        if not pollution_data.get('list'):
            raise ValueError("No pollution data in API response.")

        record = pollution_data['list'][0]
        main = record.get('main', {})
        components = record.get('components', {})
        dt = record.get('dt')

        iso_timestamp = datetime.fromtimestamp(dt, timezone.utc).isoformat() if dt else datetime.now(timezone.utc).isoformat()

        return {
            "location": f"{lat},{lon}",
            "aqi": main.get('aqi'),
            "co": components.get('co'),
            "no2": components.get('no2'),
            "o3": components.get('o3'),
            "pm2_5": components.get('pm2_5'),
            "pm10": components.get('pm10'),
            "so2": components.get('so2'),
            "timestamp": iso_timestamp,
            "provider": "openweathermap"
        }
        if not pollution_data.get('list'):
            raise ValueError("No pollution data in API response.")

        record = pollution_data['list'][0]
        main = record.get('main', {})
        components = record.get('components', {})
        dt = record.get('dt')

        iso_timestamp = datetime.fromtimestamp(dt, timezone.utc).isoformat() if dt else datetime.now(timezone.utc).isoformat()

        return {
            "location": location_name,
            "aqi": main.get('aqi'),
            "co": components.get('co'),
            "no2": components.get('no2'),
            "o3": components.get('o3'),
            "pm2_5": components.get('pm2_5'),
            "pm10": components.get('pm10'),
            "so2": components.get('so2'),
            "timestamp": iso_timestamp,
            "provider": "openweathermap"
        }
