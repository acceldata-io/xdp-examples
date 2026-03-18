#!/usr/bin/env python3
"""
Task 1: Extract Weather Data (Spark Distributed)

This task extracts weather data from Open-Meteo API, transforms it using
Spark DataFrames (distributed processing), and writes to Azure Data Lake 
Storage Gen2 as Parquet files in the staging zone.

The output serves as input for Task 2 (Transform & Load).

API Source: https://open-meteo.com/en/docs

Environment Variables (auto-injected from Kubernetes datastore secrets):
- DATASTORE_ADLS_STORAGE_ACCOUNT_NAME: Storage account name
- DATASTORE_ADLS_CONTAINER_NAME: Container/filesystem name
- DATASTORE_ADLS_CONTAINER_PATH: Output path for staging data
- DATASTORE_ADLS_TENANT_ID: Azure AD tenant ID
- DATASTORE_ADLS_CLIENT_ID: Service principal client ID
- DATASTORE_ADLS_CLIENT_SECRET: Service principal client secret
- LATITUDE: Location latitude
- LONGITUDE: Location longitude

Author: Acceldata Platform Team
Version: 3.0.0
Company: acceldata.io
"""

import os
import sys
import json
import logging
import time
from datetime import datetime
from typing import Optional
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import lit, current_timestamp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Open-Meteo API endpoint
OPEN_METEO_API = "https://api.open-meteo.com/v1/forecast"


class WeatherExtractor:
    """Extract weather data from Open-Meteo API and write to ADLS staging using Spark"""
    
    def __init__(self):
        """
        Initialize Weather Extractor
        
        All Azure configuration is automatically picked from Kubernetes datastore secrets.
        """
        self.storage_account = os.environ.get("DATASTORE_ADLS_STORAGE_ACCOUNT_NAME")
        self.container_name = os.environ.get("DATASTORE_ADLS_CONTAINER_NAME")
        self.container_path = os.environ.get("DATASTORE_ADLS_CONTAINER_PATH")
        
        # Location coordinates from Kubernetes secrets
        self.latitude = float(os.environ.get("LATITUDE", "40.7128"))
        self.longitude = float(os.environ.get("LONGITUDE", "-74.0060"))
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session with Azure support"""
        logger.info("Creating Spark session with Azure configuration...")
        
        # Note: Azure OAuth credentials are automatically picked from Kubernetes datastore secrets
        # No need to explicitly configure spark.hadoop.* settings here
        
        spark = SparkSession.builder \
            .appName("Weather ETL - Task 1 Extract - Acceldata") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        # Set master to local if not already set (for local testing)
        if not spark.sparkContext.master:
            spark.conf.set("spark.master", "local[*]")
        
        logger.info("✓ Spark session created successfully")
        return spark
    
    def build_abfs_path(self, file_path: str) -> str:
        """
        Build the ABFS (Azure Blob File System) path
        
        Args:
            file_path: Path to the file within the container
            
        Returns:
            Full ABFS path
        """
        return f"abfss://{self.container_name}@{self.storage_account}.dfs.core.windows.net/{file_path}"
    
    def extract_from_api(self) -> dict:
        """
        Extract weather data from Open-Meteo API (runs on driver)
        
        Returns:
            Dictionary containing weather data from API
        """
        logger.info("=" * 60)
        logger.info("TASK 1: EXTRACT - Fetching weather data from Open-Meteo")
        logger.info("=" * 60)
        logger.info(f"Location: lat={self.latitude}, lon={self.longitude}")
        
        # Build API request with current weather and hourly forecast
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "current": [
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "precipitation",
                "weather_code",
                "cloud_cover",
                "wind_speed_10m",
                "wind_direction_10m",
                "wind_gusts_10m"
            ],
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation_probability",
                "precipitation",
                "weather_code",
                "wind_speed_10m"
            ],
            "daily": [
                "weather_code",
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "wind_speed_10m_max"
            ],
            "timezone": "auto",
            "forecast_days": 7
        }
        
        # Retry logic for transient API errors (502, 503, 504)
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                response = requests.get(OPEN_METEO_API, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                break  # Success, exit retry loop
            except requests.exceptions.HTTPError as e:
                if response.status_code in [502, 503, 504] and attempt < max_retries - 1:
                    logger.warning(f"API returned {response.status_code}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    raise
        
        logger.info(f"✓ Fetched weather data from Open-Meteo API")
        logger.info(f"  Timezone: {data.get('timezone')}")
        logger.info(f"  Elevation: {data.get('elevation')}m")
        
        return data
    
    def transform_to_dataframe(self, data: dict) -> DataFrame:
        """
        Transform API response to Spark DataFrame (distributed operation)
        
        Args:
            data: Raw weather data from API
            
        Returns:
            Spark DataFrame with weather data
        """
        logger.info("Transforming data using Spark DataFrame (distributed)...")
        
        # Extract hourly data and create records
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        
        # Create list of row dictionaries
        rows = []
        for i, timestamp in enumerate(times):
            row = {
                "timestamp": timestamp,
                "temperature_2m": hourly.get("temperature_2m", [None] * len(times))[i],
                "relative_humidity_2m": hourly.get("relative_humidity_2m", [None] * len(times))[i],
                "precipitation_probability": hourly.get("precipitation_probability", [None] * len(times))[i],
                "precipitation": hourly.get("precipitation", [None] * len(times))[i],
                "weather_code": hourly.get("weather_code", [None] * len(times))[i],
                "wind_speed_10m": hourly.get("wind_speed_10m", [None] * len(times))[i],
            }
            rows.append(row)
        
        # Define schema for better performance
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("temperature_2m", DoubleType(), True),
            StructField("relative_humidity_2m", IntegerType(), True),
            StructField("precipitation_probability", IntegerType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("weather_code", IntegerType(), True),
            StructField("wind_speed_10m", DoubleType(), True),
        ])
        
        # Create DataFrame from rows - THIS TRIGGERS EXECUTOR USAGE
        df = self.spark.createDataFrame(rows, schema)
        
        # Add metadata columns (Spark transformations - distributed)
        df = df.withColumn("latitude", lit(data.get("latitude"))) \
               .withColumn("longitude", lit(data.get("longitude"))) \
               .withColumn("elevation", lit(data.get("elevation"))) \
               .withColumn("timezone", lit(data.get("timezone"))) \
               .withColumn("_extracted_at", current_timestamp()) \
               .withColumn("_source", lit(OPEN_METEO_API))
        
        # Repartition to ensure distributed processing
        df = df.repartition(4)
        
        record_count = df.count()  # Action - triggers distributed computation
        logger.info(f"✓ Created Spark DataFrame with {record_count} hourly records")
        logger.info(f"  Partitions: {df.rdd.getNumPartitions()}")
        
        return df
    
    def write_parquet(
        self,
        df: DataFrame,
        file_path: str,
        mode: str = "overwrite",
        partition_by: Optional[list] = None,
        compression: str = "snappy"
    ) -> None:
        """
        Write DataFrame to Parquet file in Azure Data Lake Storage
        
        Args:
            df: Spark DataFrame to write
            file_path: Path to write the Parquet file
            mode: Write mode (overwrite, append, errorifexists, ignore)
            partition_by: Optional list of columns to partition by
            compression: Compression codec (snappy, gzip, lzo, etc.)
        """
        abfs_path = self.build_abfs_path(file_path)
        logger.info(f"Writing Parquet to: {abfs_path}")
        
        writer = df.write \
            .option("compression", compression) \
            .mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.parquet(abfs_path)
        
        logger.info(f"✓ Parquet written successfully to {abfs_path}")
    
    def run(self) -> dict:
        """Execute the extract task with distributed Spark processing"""
        try:
            # Step 1: Extract data from API (driver-only, as expected)
            raw_data = self.extract_from_api()
            
            # Step 2: Transform to Spark DataFrame (distributed)
            df = self.transform_to_dataframe(raw_data)
            
            # Show sample of data
            logger.info("Sample data to be written:")
            df.show(5, truncate=False)
            logger.info("Schema:")
            df.printSchema()
            
            # Step 3: Write to ADLS staging (distributed)
            now = datetime.utcnow()
            staging_path = f"{self.container_path}/staging/year={now.year}/month={now.month:02d}/day={now.day:02d}/weather_hourly"
            
            self.write_parquet(df, staging_path)
            
            result = {
                "status": "success",
                "task": "extract",
                "output_path": staging_path,
                "container": self.container_name,
                "location": {
                    "latitude": self.latitude,
                    "longitude": self.longitude
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            
            logger.info("=" * 60)
            logger.info("✓ TASK 1 COMPLETE: Extract succeeded (Spark distributed)")
            logger.info(f"  Output: {self.container_name}/{staging_path}")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"Extract failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")
    
    def close(self) -> None:
        """Close Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main function to run Weather ETL Extract"""
    logger.info("=" * 60)
    logger.info("Weather ETL - Task 1 Extract - Acceldata")
    logger.info("=" * 60)
    logger.info("Company: acceldata.io")
    logger.info("Configuration loaded from Kubernetes datastore secrets")
    logger.info("=" * 60)
    
    extractor = WeatherExtractor()
    result = extractor.run()
    
    # Print result as JSON for pipeline orchestration
    print(json.dumps(result))
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
