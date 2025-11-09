from pydantic_settings import BaseSettings

class TrafficClientConfig(BaseSettings):
    provider: str = "mock"  # tomtom, mock
    api_key: str = ""
    base_url: str = ""
    timeout: int = 30
    
    class Config:
        env_prefix = "TRAFFIC_"

class WeatherClientConfig(BaseSettings):
    provider: str = "mock"  # mock (add more later)
    api_key: str = ""
    base_url: str = ""
    timeout: int = 30
    
    class Config:
        env_prefix = "WEATHER_"

class AppConfig(BaseSettings):
    app_name: str = "API Service"
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = 8000
    
    traffic: TrafficClientConfig = TrafficClientConfig()
    weather: WeatherClientConfig = WeatherClientConfig()

def get_config():
    return AppConfig()