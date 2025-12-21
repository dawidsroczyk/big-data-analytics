from .mock import MockWeatherClient
from .openweathermap import OpenWeatherMapClient  # added

WEATHER_CLIENTS = {
    "mock": MockWeatherClient,
    "openweathermap": OpenWeatherMapClient,            # added
    "openweathermap_legacy": OpenWeatherMapClient      # alias for compatibility (optional)
}

def create_weather_client(provider: str, config):
    client_class = WEATHER_CLIENTS.get(provider)
    if not client_class:
        raise ValueError(f"Unknown weather provider: {provider}")
    return client_class(config)