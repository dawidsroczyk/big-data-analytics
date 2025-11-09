import asyncio
import random
from datetime import datetime
from ..base import BaseTrafficClient, ClientConfig
from typing import Dict, Any

class MockTrafficClient(BaseTrafficClient):
    """Mock traffic data for development and testing"""
    
    async def get_traffic_flow(self, lat: float, lng: float, radius: int = 1000) -> Dict[str, Any]:
        await asyncio.sleep(0.1)
        
        return {
            "free_flow_speed": random.randint(20, 80),
            "current_travel_time": random.randint(5, 30),
            "free_flow_travel_time": random.randint(5, 30),
            "road_closure": random.choice([True, False]),
            "location": f"{lat},{lng}",
            "provider": "mock"
        }