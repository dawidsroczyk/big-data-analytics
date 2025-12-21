import httpx
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from ..base import BaseWeatherClient, ClientConfig

class OpenWeatherMapClient(BaseWeatherClient):
    """OpenWeatherMap v2.5 client (async)."""

    WEATHER_PATH = "/weather"
    FORECAST_PATH = "/forecast"  # simple 3h-step forecast endpoint

    def __init__(self, config: ClientConfig, units: str = "metric", lang: str = "en"):
        """
        Args:
            config: ClientConfig instance (api_key, base_url, timeout)
            units: 'metric', 'imperial', or 'standard'
            lang: language for description
        """
        super().__init__(config)
        self.units = units if units in ("metric", "imperial", "standard") else "metric"
        self.lang = lang

    async def get_current_weather(self, lat: float, lng: float) -> Dict[str, Any]:
        """Fetch current weather for coordinates. Returns normalized dict similar to MockWeatherClient."""
        base = self.config.base_url.rstrip('/')
        url = f"{base}{self.WEATHER_PATH}"
        params = {
            "lat": lat,
            "lon": lng,
            "appid": self.config.api_key,
            "units": self.units,
            "lang": self.lang
        }

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        # Defensive parsing with sensible defaults
        main = data.get("main", {}) or {}
        wind = data.get("wind", {}) or {}
        weather_list = data.get("weather") or []
        weather0 = weather_list[0] if isinstance(weather_list, list) and weather_list else {}

        dt = data.get("dt")
        timestamp_iso = None
        if isinstance(dt, (int, float)):
            timestamp_iso = datetime.fromtimestamp(int(dt), tz=timezone.utc).isoformat()
        else:
            # fallback to current UTC time
            timestamp_iso = datetime.now(timezone.utc).isoformat()

        name = data.get("name") or ""
        country = (data.get("sys") or {}).get("country") or ""
        location = f"{name}, {country}".strip(", ") if name else f"{lat},{lng}"

        return {
            "temperature": float(main.get("temp", 0.0)),
            "conditions": (weather0.get("description") or "").title(),
            "humidity": float(main.get("humidity", 0.0)),
            "wind_speed": float(wind.get("speed", 0.0)),
            "pressure": main.get("pressure"),
            "location": location,
            "dt": int(dt) if isinstance(dt, (int, float)) else None,
            "timestamp": timestamp_iso,
            "provider": "openweathermap"
        }

    async def get_forecast(self, lat: float, lng: float, days: int = 5) -> Dict[str, Any]:
        """
        Basic forecast fetch using /forecast (3-hour steps).
        Returns a simplified list of daily summaries (up to `days` entries).
        """
        base = self.config.base_url.rstrip('/')
        url = f"{base}{self.FORECAST_PATH}"
        params = {
            "lat": lat,
            "lon": lng,
            "appid": self.config.api_key,
            "units": self.units,
            "lang": self.lang
        }

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        raw_list = data.get("list", []) or []
        # Aggregate simple daily summaries by date (UTC)
        daily: Dict[str, Dict[str, Any]] = {}
        for item in raw_list:
            dt = item.get("dt")
            if not isinstance(dt, (int, float)):
                continue
            ts = datetime.fromtimestamp(int(dt), tz=timezone.utc)
            date_key = ts.date().isoformat()
            main = item.get("main", {}) or {}
            weather0 = (item.get("weather") or [{}])[0] if item.get("weather") else {}
            entry = {
                "temp": float(main.get("temp", 0.0)),
                "description": (weather0.get("description") or "").title()
            }
            # keep a simple example: first occurrence per date
            if date_key not in daily:
                daily[date_key] = {
                    "date": date_key,
                    "temperature": entry["temp"],
                    "conditions": entry["description"]
                }
            # stop early if we have enough days
            if len(daily) >= days:
                break

        forecast_list: List[Dict[str, Any]] = list(daily.values())
        return {
            "forecast": forecast_list,
            "location": f"{(data.get('city') or {}).get('name', f'{lat},{lng}')}",
            "provider": "openweathermap"
        }
