from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..container import container

router = APIRouter()

class TrafficResponse(BaseModel):
    location: str
    free_flow_speed: float
    current_travel_time: int
    free_flow_travel_time: int
    road_closure: bool
    updated_at: str
    data_provider: str

@router.get("/traffic", response_model=TrafficResponse)
async def get_traffic(lat: float, lng: float):
    try:
        repo = container.traffic_repository()
        traffic_info = await repo.get_traffic_conditions(lat, lng)
        return TrafficResponse(
            location=traffic_info.location,
            free_flow_speed=traffic_info.free_flow_speed,
            current_travel_time=traffic_info.current_travel_time,
            free_flow_travel_time=traffic_info.free_flow_travel_time,
            road_closure=traffic_info.road_closure,
            updated_at=traffic_info.updated_at.isoformat(),
            data_provider=traffic_info.data_provider
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))