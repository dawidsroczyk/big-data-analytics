from .mock import MockWeatherClient

WEATHER_CLIENTS = {
    "mock": MockWeatherClient
}

def create_weather_client(provider: str, config):
    client_class = WEATHER_CLIENTS.get(provider)
    if not client_class:
        raise ValueError(f"Unknown weather provider: {provider}")
    return client_class(config)