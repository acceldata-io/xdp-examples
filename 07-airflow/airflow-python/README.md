# Weather ETL Pipeline - Azure ADLS Gen2

Two-task ETL pipeline using **PySpark** for extracting weather data from [Open-Meteo API](https://open-meteo.com/en/docs), transforming it, and loading to Azure Data Lake Storage Gen2.

Designed to run as a **DAG workflow on Acceldata XDP Platform**.

## Pipeline Overview

```
┌─────────────────────┐         ┌──────────────────────────┐
│   TASK 1: EXTRACT   │  ────►  │  TASK 2: TRANSFORM/LOAD  │
│   (Open-Meteo API)  │         │    (Spark + Parquet)     │
│   + Spark Session   │         │    + Spark Session       │
└─────────────────────┘         └──────────────────────────┘
         │                                   │
         ▼                                   ▼
   staging/weather_raw.json         curated/weather_hourly.parquet
```

## Data Source

**API:** [Open-Meteo Weather Forecast API](https://open-meteo.com/en/docs)

- Free, no API key required
- High-resolution weather models
- Up to 16 days forecast
- Hourly and daily data

## Project Structure

```
airflow-python/
├── task1-extract/
│   ├── main.py              # Extract: Open-Meteo → ADLS staging (JSON)
│   ├── Dockerfile           # Spark-enabled image
│   └── requirements.txt
├── task2-transform-load/
│   ├── main.py              # Transform + Load: staging → curated (Parquet)
│   ├── Dockerfile           # Spark-enabled image
│   └── requirements.txt
└── README.md
```

## Task Details

### Task 1: Extract Weather Data

| Property | Value |
|----------|-------|
| **Image** | `acceldata/weather-etl-task1:1.0.0` |
| **Base Image** | `spark-python-jdk-ubuntu:3.5.5.3.3.6.2-1-ubuntu-20` |
| **Input** | Open-Meteo API |
| **Output** | `{CONTAINER_PATH}/staging/.../weather_raw.json` |
| **Data** | Current weather + 7-day hourly forecast |

### Task 2: Transform & Load

| Property | Value |
|----------|-------|
| **Image** | `acceldata/weather-etl-task2:1.0.0` |
| **Base Image** | `spark-python-jdk-ubuntu:3.5.5.3.3.6.2-1-ubuntu-20` |
| **Input** | `{CONTAINER_PATH}/staging/.../weather_raw.json` |
| **Output** | `{CONTAINER_PATH}/curated/.../weather_hourly.parquet` |
| **Engine** | Apache Spark (DataFrame API) |

## Environment Variables (from Kubernetes Datastore Secrets)

All credentials are injected from Kubernetes secrets. **No fallback values** - tasks will fail if secrets are missing.

### Required for Both Tasks

| Variable | Description |
|----------|-------------|
| `DATASTORE_ADLS_STORAGE_ACCOUNT_NAME` | Storage account name |
| `DATASTORE_ADLS_CONTAINER_NAME` | Container/filesystem name |
| `DATASTORE_ADLS_CONTAINER_PATH` | Base path for data |
| `DATASTORE_ADLS_TENANT_ID` | Azure AD tenant ID |
| `DATASTORE_ADLS_CLIENT_ID` | Service principal client ID |
| `DATASTORE_ADLS_CLIENT_SECRET` | Service principal client secret |

### Required for Task 1

| Variable | Description |
|----------|-------------|
| `LATITUDE` | Location latitude |
| `LONGITUDE` | Location longitude |

### Optional for Task 2

| Variable | Description |
|----------|-------------|
| `INPUT_PATH` | Override staging path (auto-detected if not set) |

## Build & Push Images

```bash
cd xdp-playgroud/azure-blob/airflow-python

# Build Task 1
cd task1-extract
docker build -t acceldata/weather-etl-task1:1.0.0 .
docker push acceldata/weather-etl-task1:1.0.0

# Build Task 2
cd ../task2-transform-load
docker build -t acceldata/weather-etl-task2:1.0.0 .
docker push acceldata/weather-etl-task2:1.0.0
```

## Deploy to XDP Platform

### 1. Set Kubeconfig

```bash
export KUBECONFIG=/Users/ravichandracr/Downloads/xdp-dev.yaml
kubectl config set-context --current --namespace=akshay-xdp101
```

### 2. Create Secret for Azure Credentials

All environment variables are required (no defaults):

```bash
kubectl create secret generic weather-etl-datastore-secret \
  --from-literal=DATASTORE_ADLS_STORAGE_ACCOUNT_NAME="testadlsxdp" \
  --from-literal=DATASTORE_ADLS_CONTAINER_NAME="weather" \
  --from-literal=DATASTORE_ADLS_CONTAINER_PATH="weather-data" \
  --from-literal=DATASTORE_ADLS_TENANT_ID="your-tenant-id" \
  --from-literal=DATASTORE_ADLS_CLIENT_ID="your-client-id" \
  --from-literal=DATASTORE_ADLS_CLIENT_SECRET="your-client-secret" \
  --from-literal=LATITUDE="52.52" \
  --from-literal=LONGITUDE="13.41" \
  -n akshay-xdp101
```

### 3. Configure DAG in XDP Platform

In XDP Airflow, create a DAG with two tasks:

**Task 1 Configuration:**
- Image: `acceldata/weather-etl-task1:1.0.0`
- Secret: `weather-etl-datastore-secret`

**Task 2 Configuration:**
- Image: `acceldata/weather-etl-task2:1.0.0`
- Secret: `weather-etl-datastore-secret`
- Depends on: Task 1

## Output Schema

Final Parquet schema (`weather_hourly.parquet`):

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | string | Forecast time (ISO 8601) |
| `temperature_c` | double | Temperature in Celsius |
| `humidity_percent` | int | Relative humidity % |
| `precipitation_probability` | int | Chance of precipitation % |
| `precipitation_mm` | double | Precipitation amount |
| `weather_code` | int | WMO weather code |
| `weather_description` | string | Human-readable weather |
| `wind_speed_kmh` | double | Wind speed km/h |
| `latitude` | double | Location latitude |
| `longitude` | double | Location longitude |
| `elevation_m` | double | Elevation in meters |
| `timezone` | string | Location timezone |
| `_transformed_at` | string | ETL timestamp |
| `_pipeline` | string | Pipeline name |
| `_version` | string | Pipeline version |

## ABFS Path Format

Data is written using ABFS (Azure Blob File System) paths:

```
abfss://{container}@{storage_account}.dfs.core.windows.net/{path}
```

Example:
```
abfss://weather@testadlsxdp.dfs.core.windows.net/weather-data/curated/year=2026/month=01/day=16/weather_hourly.parquet
```

## Sample Locations

| City | Latitude | Longitude |
|------|----------|-----------|
| Berlin | 52.52 | 13.41 |
| New York | 40.7128 | -74.0060 |
| London | 51.5074 | -0.1278 |
| Tokyo | 35.6762 | 139.6503 |
| Sydney | -33.8688 | 151.2093 |

## License

Copyright © 2024 Acceldata Inc. All rights reserved.
