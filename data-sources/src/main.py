from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .config import get_config
from .routers import traffic, weather, health

def create_application() -> FastAPI:
    config = get_config()
    
    app = FastAPI(
        title=config.app_name,
        debug=config.debug,
        version="1.0.0"
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.include_router(traffic.router, prefix="/api/v1", tags=["traffic"])
    app.include_router(weather.router, prefix="/api/v1", tags=["weather"])
    app.include_router(health.router, prefix="/api/v1", tags=["health"])
    
    @app.get("/")
    async def root():
        return {
            "message": "API Service",
            "status": "running",
            "endpoints": {
                "traffic": "/api/v1/traffic?lat=40.7128&lng=-74.0060",
                "weather": "/api/v1/weather?lat=40.7128&lng=-74.0060",
                "health": "/api/v1/health"
            }
        }
    
    return app

app = create_application()