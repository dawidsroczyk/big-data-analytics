import asyncio
import random
from datetime import datetime
from ..base import BaseTrafficClient, ClientConfig
from typing import Dict, Any

class MockTrafficClient(BaseTrafficClient):
    """Mock traffic data for development and testing"""
    
    async def get_traffic_flow(self, lat: float, lng: float, radius: int = 1000) -> Dict[str, Any]:
        await asyncio.sleep(0.1)  # Simulate API delay
        
        return {
            "flow_rate": random.randint(500, 2000),
            "congestion_level": random.choice(["low", "medium", "high"]),
            "average_speed": random.randint(20, 100),
            "location": f"{lat},{lng}",
            "timestamp": datetime.now().isoformat(),
            "provider": "mock"
        }
    
    async def get_incidents(self, lat: float, lng: float, radius: int = 5000) -> Dict[str, Any]:
        await asyncio.sleep(0.1)
        
        incident_count = random.randint(0, 5)
        return {
            "incidents": [
                {
                    "id": i,
                    "type": random.choice(["accident", "roadwork", "closure"]),
                    "severity": random.choice(["low", "medium", "high"]),
                    "location": f"{lat + random.uniform(-0.01, 0.01)},{lng + random.uniform(-0.01, 0.01)}"
                } for i in range(incident_count)
            ],
            "timestamp": datetime.now().isoformat(),
            "provider": "mock"
        }