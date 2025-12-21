# clients/air_pollution/__init__.py
from .mock import MockAirPollutionClient
from .openweathermap import OpenWeatherMapAirPollutionClient

AIR_POLLUTION_CLIENTS = {
    "mock": MockAirPollutionClient,
    "openweathermap": OpenWeatherMapAirPollutionClient,
}

def create_air_pollution_client(provider: str, config):
    client_class = AIR_POLLUTION_CLIENTS.get(provider)
    if not client_class:
        raise ValueError(f"Unknown air-pollution provider: {provider}")
    return client_class(config)
