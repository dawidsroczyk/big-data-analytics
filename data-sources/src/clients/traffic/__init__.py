from .tomtom import TomTomTrafficClient
from .mock import MockTrafficClient

TRAFFIC_CLIENTS = {
    "tomtom": TomTomTrafficClient,
    "mock": MockTrafficClient
}

def create_traffic_client(provider: str, config):
    client_class = TRAFFIC_CLIENTS.get(provider)
    if not client_class:
        raise ValueError(f"Unknown traffic provider: {provider}")
    return client_class(config)