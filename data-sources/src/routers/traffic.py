from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..container import container

router = APIRouter()

class TrafficResponse(BaseModel):
    location: str
    flow_rate: float
    congestion_level: str
    incident_count: int
    updated_at: str
    data_provider: str

@router.get("/traffic", response_model=TrafficResponse)
async def get_traffic(lat: float, lng: float):
    try:
        repo = container.traffic_repository()
        traffic_info = await repo.get_traffic_conditions(lat, lng)
        return TrafficResponse(
            location=traffic_info.location,
            flow_rate=traffic_info.flow_rate,
            congestion_level=traffic_info.congestion_level,
            incident_count=traffic_info.incident_count,
            updated_at=traffic_info.updated_at.isoformat(),
            data_provider=traffic_info.data_provider
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))