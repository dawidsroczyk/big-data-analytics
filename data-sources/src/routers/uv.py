from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..container import container

router = APIRouter()

class UVResponse(BaseModel):
    uv_index: float
    location: str
    timestamp: str
    data_provider: str

@router.get("/uv", response_model=UVResponse)
async def get_uv(lat: float, lng: float = None, lon: float = None):
    try:
        # accept both 'lng' and 'lon' for compatibility with existing flows
        longitude = lon if lon is not None else lng
        if longitude is None:
            raise ValueError("Missing longitude parameter (provide 'lng' or 'lon')")

        repo = container.uv_repository()
        uv_info = await repo.get_uv_index(lat, longitude)
        return UVResponse(
            uv_index=uv_info.uv_index,
            location=uv_info.location,
            timestamp=uv_info.timestamp.isoformat(),
            data_provider=uv_info.provider
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
