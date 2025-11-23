# clients/air_pollution/mock.py
import asyncio
import random
from datetime import datetime
from ..base import BaseAirPollutionClient, ClientConfig
from typing import Dict, Any

class MockAirPollutionClient(BaseAirPollutionClient):
    """Mock air pollution data for development and testing"""
    
    async def get_current_air_pollution(self, lat: float, lon: float) -> Dict[str, Any]:
            await asyncio.sleep(0.1)
            now = datetime.now()
            return {
                "location": f"{lat},{lon}",
                "aqi": random.randint(1, 5),  # Air Quality Index (1=Good, 5=Very Poor)
                "co": round(random.uniform(0, 10), 2),
                "no2": round(random.uniform(0, 200), 1),
                "o3": round(random.uniform(0, 300), 1),
                "pm2_5": round(random.uniform(0, 100), 1),
                "pm10": round(random.uniform(0, 150), 1),
                "so2": round(random.uniform(0, 50), 1),
                "dt": int(now.timestamp()),              # UNIX timestamp
                "timestamp": now.isoformat(),            # optional, human readable
                "provider": "mock"
            }
