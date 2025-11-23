import httpx
from ..base import BaseTrafficClient, ClientConfig
from typing import Dict, Any

class TomTomTrafficClient(BaseTrafficClient):
    """TomTom Traffic API implementation"""
    
    async def get_traffic_flow(self, lat: float, lng: float, radius: int = 1000) -> Dict[str, Any]:
        url = f"{self.config.base_url}/traffic/services/4/flowSegmentData/relative0/10/json"
        
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            response = await client.get(
                url,
                params={
                    "point": f"{lat},{lng}",
                    "unit": "KMPH",
                    "key": self.config.api_key
                }
            )
            response.raise_for_status()
            data = response.json()
            fsd = data.get("flowSegmentData", {})
            return {
                "free_flow_speed": fsd.get("freeFlowSpeed"),
                "current_travel_time": fsd.get("currentTravelTime"),
                "free_flow_travel_time": fsd.get("freeFlowTravelTime"),
                "road_closure": fsd.get("roadClosure"),
                "location": f"{lat},{lng}",
                "provider": "tomtom",
                "timestamp": fsd.get("timestamp") 
            }