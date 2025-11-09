import asyncio
import random
from datetime import datetime
from ..base import BaseWeatherClient, ClientConfig
from typing import Dict, Any

class MockWeatherClient(BaseWeatherClient):
    """Mock weather data for development and testing"""
    
    async def get_current_weather(self, lat: float, lng: float) -> Dict[str, Any]:
        await asyncio.sleep(0.1)
        
        conditions = ["sunny", "cloudy", "rainy", "snowy", "foggy"]
        return {
            "temperature": round(random.uniform(-10, 35), 1),
            "conditions": random.choice(conditions),
            "humidity": random.randint(30, 90),
            "wind_speed": round(random.uniform(0, 25), 1),
            "pressure": random.randint(980, 1030),
            "location": f"{lat},{lng}",
            "timestamp": datetime.now().isoformat(),
            "provider": "mock"
        }
    
    async def get_forecast(self, lat: float, lng: float, days: int = 5) -> Dict[str, Any]:
        await asyncio.sleep(0.1)
        
        return {
            "forecast": [
                {
                    "date": f"Day {i}",
                    "temperature": round(random.uniform(-10, 35), 1),
                    "conditions": random.choice(["sunny", "cloudy", "rainy"])
                } for i in range(days)
            ],
            "location": f"{lat},{lng}",
            "provider": "mock"
        }