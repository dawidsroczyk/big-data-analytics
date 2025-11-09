from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict
from typing import Optional
import os

class TrafficClientConfig(BaseSettings):
    provider: str = Field(..., description="Traffic provider (tomtom, mock)")
    api_key: str = Field(..., description="API key for traffic service")
    base_url: str = Field(..., description="Base URL for traffic API")
    timeout: int = Field(default=30, description="Timeout in seconds")
    
    model_config = ConfigDict(env_prefix="TRAFFIC_")

class WeatherClientConfig(BaseSettings):
    provider: str = Field(..., description="Weather provider (mock)")
    api_key: str = Field(..., description="API key for weather service")
    base_url: str = Field(..., description="Base URL for weather API")
    timeout: int = Field(default=30, description="Timeout in seconds")
    
    model_config = ConfigDict(env_prefix="WEATHER_")

class AppConfig(BaseSettings):
    app_name: str = Field(..., description="Application name")
    debug: bool = Field(default=False, description="Debug mode")
    host: str = Field(default="0.0.0.0", description="Host to bind")
    port: int = Field(default=8000, description="Port to bind")
    
    traffic: Optional[TrafficClientConfig] = None
    weather: Optional[WeatherClientConfig] = None

def get_config():
    traffic_config = TrafficClientConfig()
    weather_config = WeatherClientConfig()
    
    app_config = AppConfig(
        traffic=traffic_config,
        weather=weather_config
    )

    return app_config