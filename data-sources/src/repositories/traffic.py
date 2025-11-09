from dataclasses import dataclass
from datetime import datetime
from typing import List
from ..clients.base import BaseTrafficClient

@dataclass
class TrafficInfo:
    location: str
    free_flow_speed: float
    current_travel_time: int
    free_flow_travel_time: int
    road_closure: bool
    updated_at: datetime
    data_provider: str

class TrafficRepository:
    def __init__(self, traffic_client: BaseTrafficClient):
        self.client = traffic_client
    
    async def get_traffic_conditions(self, lat: float, lng: float) -> TrafficInfo:
        flow_data = await self.client.get_traffic_flow(lat, lng)
        
        return TrafficInfo(
            location=flow_data.get('location', f"{lat},{lng}"),
            free_flow_speed=flow_data.get('free_flow_speed', 0),
            current_travel_time=flow_data.get('current_travel_time', 0),
            free_flow_travel_time=flow_data.get('free_flow_travel_time', 0),
            road_closure=flow_data.get('road_closure', False),
            updated_at=datetime.now(),
            data_provider=flow_data.get('provider', 'unknown')
        )