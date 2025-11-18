from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from dataclasses import dataclass

@dataclass
class ClientConfig:
    api_key: str
    base_url: str
    timeout: int = 30

class BaseTrafficClient(ABC):
    def __init__(self, config: ClientConfig):
        self.config = config
    
    @abstractmethod
    async def get_traffic_flow(self, lat: float, lng: float, radius: int = 1000) -> Dict[str, Any]:
        pass

class BaseWeatherClient(ABC):
    def __init__(self, config: ClientConfig):
        self.config = config
    
    @abstractmethod
    async def get_current_weather(self, lat: float, lng: float) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def get_forecast(self, lat: float, lng: float, days: int = 5) -> Dict[str, Any]:
        pass

class BaseAirClient:
    """Base class for air pollution clients.

    Accepts a `ClientConfig` instance for consistency with other clients.
    """
    def __init__(self, config: ClientConfig):
        # Accept either a ClientConfig or a dict-like object with the same attributes
        if isinstance(config, ClientConfig):
            self.api_key = config.api_key
            self.base_url = config.base_url.rstrip("/")
            self.timeout = config.timeout
        else:
            # fallback: try attribute access for compatibility
            self.api_key = getattr(config, "api_key")
            self.base_url = getattr(config, "base_url").rstrip("/")
            self.timeout = getattr(config, "timeout", 30)
