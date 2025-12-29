from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict
from typing import Optional


class TrafficClientConfig(BaseSettings):
    provider: str = Field(..., description="Traffic provider (tomtom, mock)")
    api_key: str = Field(..., description="API key for traffic service")
    base_url: str = Field(..., description="Base URL for traffic API")
    timeout: int = Field(default=30, description="Timeout in seconds")

    model_config = ConfigDict(env_prefix="TRAFFIC_")


class WeatherClientConfig(BaseSettings):
    provider: str = Field(..., description="Weather provider (mock, openweathermap)")
    api_key: str = Field(..., description="API key for weather service")
    base_url: str = Field(..., description="Base URL for weather API")
    timeout: int = Field(default=30, description="Timeout in seconds")

    model_config = ConfigDict(env_prefix="WEATHER_")


class AirPollutionClientConfig(BaseSettings):
    provider: str = Field(..., description="Air pollution provider (mock, openweathermap)")
    api_key: str = Field(..., description="API key for air pollution service")
    base_url: str = Field(..., description="Base URL for air pollution API")
    timeout: int = Field(default=30, description="Timeout in seconds")

    model_config = ConfigDict(env_prefix="AIR_POLLUTION_")

class UVClientConfig(BaseSettings):
    provider: str = Field(..., description="UV provider (mock, openweathermap)")
    api_key: str = Field(..., description="API key for UV index service")
    base_url: str = Field(..., description="Base URL for UV API (e.g. https://api.openweathermap.org/data/2.5)")
    timeout: int = Field(default=30)

    model_config = ConfigDict(env_prefix="UV_")


class AppConfig(BaseSettings):
    app_name: str = Field(..., description="Application name")
    debug: bool = Field(default=False, description="Debug mode")
    host: str = Field(default="0.0.0.0", description="Host to bind")
    port: int = Field(default=8000, description="Port to bind")

    traffic: Optional[TrafficClientConfig] = None
    weather: Optional[WeatherClientConfig] = None
    air_pollution: Optional[AirPollutionClientConfig] = None
    uv: Optional[UVClientConfig] = None


def get_config() -> AppConfig:
    """Construct AppConfig by reading sub-configs from environment.

    This mirrors the previous approach but keeps construction explicit and
    avoids syntax issues.
    """
    traffic_config = TrafficClientConfig()
    weather_config = WeatherClientConfig()
    air_pollution_config = AirPollutionClientConfig()
    uv_config = UVClientConfig()

    app_config = AppConfig(
        traffic=traffic_config,
        weather=weather_config,
        air_pollution=air_pollution_config,
        uv=uv_config
    )

    return app_config