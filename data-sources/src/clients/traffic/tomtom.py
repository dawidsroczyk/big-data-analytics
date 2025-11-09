import httpx
from ..base import BaseAPIClient, ClientConfig
from typing import Dict, Any

class TomTomTrafficClient(BaseAPIClient):
    """TomTom Traffic API implementation"""
    
    async def get_traffic_flow(self, lat: float, lng: float, radius: int = 1000) -> Dict[str, Any]:
        url = f"{self.config.base_url}/flowSegmentData/relative0/10/json"
        
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            response = await client.get(
                url,
                params={
                    "point": f"{lat},{lng}",
                    "unit": "KMPH",
                    "openLr": "false",
                    "key": self.config.api_key
                }
            )
            response.raise_for_status()
            return response.json()
    
    async def get_incidents(self, lat: float, lng: float, radius: int = 5000) -> Dict[str, Any]:
        url = f"{self.config.base_url}/incidentDetails"
        
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            response = await client.get(
                url,
                params={
                    "bbox": f"{lng-0.1},{lat-0.1},{lng+0.1},{lat+0.1}",
                    "fields": "{incidents{type,geometry}}",
                    "key": self.config.api_key
                }
            )
            response.raise_for_status()
            return response.json()