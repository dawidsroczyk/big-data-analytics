from .openweather import OpenWeatherAirPollutionClient

def create_air_pollution_client(provider: str, config):
    provider = provider.lower()

    if provider == "openweather":
        return OpenWeatherAirPollutionClient(config)

    raise ValueError(f"Unknown air pollution provider: {provider}")
