from dependency_injector import containers, providers
from .config import get_config
from .clients.base import ClientConfig
from .clients.traffic import create_traffic_client
from .clients.weather import create_weather_client
from .clients.air_pollution import create_air_pollution_client
from .repositories.traffic import TrafficRepository
from .repositories.weather import WeatherRepository
from .repositories.air_pollution import AirPollutionRepository

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

    air_pollution_client_config = providers.Singleton(
        ClientConfig,
        api_key=config.provided.air_pollution.api_key,
        base_url=config.provided.air_pollution.base_url,
        timeout=config.provided.air_pollution.timeout
    )
    
    # Client factories
    traffic_client = providers.Singleton(
        create_traffic_client,
        provider=providers.Callable(lambda c: c.traffic.provider, config),
        config=traffic_client_config
    )
    
    weather_client = providers.Singleton(
        create_weather_client,
        provider=providers.Callable(lambda c: c.weather.provider, config),
        config=weather_client_config
    )
    
    air_pollution_client = providers.Singleton(
        create_air_pollution_client,
        provider=providers.Callable(lambda c: c.air_pollution.provider, config),
        config=air_pollution_client_config
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
    air_pollution_repository = providers.Factory(
        AirPollutionRepository,
        air_client=air_pollution_client
    )


container = Container()