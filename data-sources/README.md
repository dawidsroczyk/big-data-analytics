# Big Data Analytics API Service

## Quick Setup

1. **Copy the example environment file:**

   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` to set your configuration.**

---

## Environment Variables

| Variable            | Description                                 | Example Value                        |
|---------------------|---------------------------------------------|--------------------------------------|
| APP_NAME            | Application name                            | Navigation API                       |
| DEBUG               | Enable debug mode (`true`/`false`)          | false                                |
| HOST                | Host to bind                                | 0.0.0.0                              |
| PORT                | Port to bind                                | 8000                                 |
| TRAFFIC_PROVIDER    | Traffic provider (`mock`, `tomtom`)         | mock                                 |
| TRAFFIC_API_KEY     | API key for traffic provider                | your-traffic-api-key                 |
| TRAFFIC_BASE_URL    | Base URL for traffic API                    | https://api.tomtom.com       |
| TRAFFIC_TIMEOUT     | Timeout for traffic API (seconds)           | 30                                   |
| WEATHER_PROVIDER    | Weather provider (`mock`, `openweathermap`) | mock                                 |
| WEATHER_API_KEY     | API key for weather provider                | your-weather-api-key                 |
| WEATHER_BASE_URL    | Base URL for weather API                    | https://api.openweathermap.org/data/2.5 |
| WEATHER_TIMEOUT     | Timeout for weather API (seconds)           | 30                                   |
| UV_PROVIDER         |                                             | mock                                 |
| UV_API_KEY          |                                             | your-uv-api-key                      |
| UV_BASE_URL         |                                             | https://api.openweathermap.org/data/2.5 |
| UV_TIMEOUT          |                                             | 30                                   |
| AIR_POLLUTION_PROVIDER         |                                             | mock                                 |
| AIR_POLLUTION_API_KEY          |                                             | your-uv-api-key                      |
| AIR_POLLUTION_BASE_URL         |                                             | https://api.openweathermap.org/data/2.5 |
| AIR_POLLUTION_TIMEOUT          |                                             | 30                                   |

---

## Run with Docker

```bash
docker build -t big-data-analytics-api .
docker run --env-file .env -p 8000:8000 big-data-analytics-api
```

---

## Run with Docker Compose

```bash
docker-compose up --build
```

This will use the settings from your `.env` file.


---

## Example API Usage

### Health Check

```bash
curl "http://localhost:8000/api/v1/health"
```

### Traffic Data

```bash
curl "http://localhost:8000/api/v1/traffic?lat=40.7128&lng=-74.0060"
```

### Weather Data

```bash
curl "http://localhost:8000/api/v1/weather?lat=40.7128&lng=-74.0060"
```
