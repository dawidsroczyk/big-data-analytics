from dataclasses import dataclass
from datetime import datetime
from typing import List
from ..clients.base import BaseTrafficClient

@dataclass
class TrafficInfo:
    location: str
    flow_rate: float
    congestion_level: str
    incident_count: int
    updated_at: datetime
    data_provider: str

class TrafficRepository:
    def __init__(self, traffic_client: BaseTrafficClient):
        self.client = traffic_client
    
    async def get_traffic_conditions(self, lat: float, lng: float) -> TrafficInfo:
        flow_data = await self.client.get_traffic_flow(lat, lng)
        incidents_data = await self.client.get_incidents(lat, lng)
        
        # Normalize different API responses to common format
        return TrafficInfo(
            location=f"{lat},{lng}",
            flow_rate=flow_data.get('flow_rate', 0),
            congestion_level=flow_data.get('congestion_level', 'unknown'),
            incident_count=len(incidents_data.get('incidents', [])),
            updated_at=datetime.now(),
            data_provider=flow_data.get('provider', 'unknown')
        )