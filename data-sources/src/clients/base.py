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