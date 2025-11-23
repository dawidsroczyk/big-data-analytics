from .mock import MockUVClient
from .openwheather import OpenWeatherUVClient

UV_CLIENTS = {
    "mock": MockUVClient,
    "openweathermap": OpenWeatherUVClient,
    "openweathermap_legacy": OpenWeatherUVClient,
}

def create_uv_client(provider: str, config):
    client_class = UV_CLIENTS.get(provider)
    if not client_class:
        raise ValueError(f"Unknown UV provider: {provider}")
    return client_class(config)
