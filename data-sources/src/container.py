from dependency_injector import containers, providers
from .config import get_config, ClientConfig
from .clients.traffic import create_traffic_client
from .clients.weather import create_weather_client
from .repositories.traffic import TrafficRepository
from .repositories.weather import WeatherRepository

class Container(containers.DeclarativeContainer):
    config = providers.Singleton(get_config)
    
    # Client configurations
    traffic_client_config = providers.Singleton(
        ClientConfig,
        api_key=config.provided.traffic.api_key,
        base_url=config.provided.traffic.base_url,
        timeout=config.provided.traffic.timeout
    )
    
    weather_client_config = providers.Singleton(
        ClientConfig,
        api_key=config.provided.weather.api_key,
        base_url=config.provided.weather.base_url,
        timeout=config.provided.weather.timeout
    )
    
    # Client factories
    traffic_client = providers.Singleton(
        create_traffic_client,
        provider=config.provided.traffic.provider,
        config=traffic_client_config
    )
    
    weather_client = providers.Singleton(
        create_weather_client,
        provider=config.provided.weather.provider,
        config=weather_client_config
    )
    
    # Repositories
    traffic_repository = providers.Factory(
        TrafficRepository,
        traffic_client=traffic_client
    )
    
    weather_repository = providers.Factory(
        WeatherRepository,
        weather_client=weather_client
    )

container = Container()