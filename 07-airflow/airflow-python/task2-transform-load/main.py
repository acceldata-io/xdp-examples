#!/usr/bin/env python3
"""
Task 2: Transform & Load Weather Data (Spark Distributed)

This task reads the staging weather data created by Task 1 using Spark,
transforms it with Spark DataFrame operations (distributed processing),
and writes the final output to Azure Data Lake Storage Gen2 as Parquet.

Environment Variables (auto-injected from Kubernetes datastore secrets):
- DATASTORE_ADLS_STORAGE_ACCOUNT_NAME: Storage account name
- DATASTORE_ADLS_CONTAINER_NAME: Container/filesystem name
- DATASTORE_ADLS_CONTAINER_PATH: Base path for data
- DATASTORE_ADLS_TENANT_ID: Azure AD tenant ID
- DATASTORE_ADLS_CLIENT_ID: Service principal client ID
- DATASTORE_ADLS_CLIENT_SECRET: Service principal client secret

Author: Acceldata Platform Team
Version: 3.0.0
Company: acceldata.io
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, round as spark_round, when, current_timestamp, udf
)
from pyspark.sql.types import StringType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Weather code descriptions (WMO codes)
WEATHER_CODES = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow",
    73: "Moderate snow",
    75: "Heavy snow",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    95: "Thunderstorm",
    96: "Thunderstorm with hail"
}


class WeatherTransformLoader:
    """Transform staged weather data and load to ADLS as Parquet using Spark"""
    
    def __init__(self):
        """
        Initialize Weather Transform Loader
        
        All Azure configuration is automatically picked from Kubernetes datastore secrets.
        """
        self.storage_account = os.environ.get("DATASTORE_ADLS_STORAGE_ACCOUNT_NAME")
        self.container_name = os.environ.get("DATASTORE_ADLS_CONTAINER_NAME")
        self.container_path = os.environ.get("DATASTORE_ADLS_CONTAINER_PATH")
        
        # Optional: override staging path
        self.input_path = os.environ.get("INPUT_PATH")
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Register UDF for weather code mapping
        self._register_udfs()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session with Azure support"""
        logger.info("Creating Spark session with Azure configuration...")
        
        # Note: Azure OAuth credentials are automatically picked from Kubernetes datastore secrets
        # via sparkConf - we need to propagate spark.hadoop.* configs to Hadoop Configuration
        
        spark = SparkSession.builder \
            .appName("Weather ETL - Task 2 Transform Load - Acceldata") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        # Propagate spark.hadoop.* configs to Hadoop Configuration
        # This is required because PySpark doesn't auto-propagate these settings
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        spark_conf = spark.sparkContext.getConf()
        
        propagated_count = 0
        for key, value in spark_conf.getAll():
            if key.startswith("spark.hadoop."):
                hadoop_key = key[len("spark.hadoop."):]
                hadoop_conf.set(hadoop_key, value)
                # Log config key (not value for security)
                logger.info(f"  Propagated: {hadoop_key}")
                propagated_count += 1
        
        if propagated_count > 0:
            logger.info(f"✓ Propagated {propagated_count} Hadoop configs from sparkConf")
        else:
            logger.warning("⚠ No spark.hadoop.* configs found - ADLS auth may fail if not configured via datastore")
        
        logger.info("✓ Spark session created successfully")
        return spark
    
    def _register_udfs(self):
        """Register User Defined Functions for Spark"""
        # Broadcast weather codes for efficient lookup
        weather_codes_broadcast = self.spark.sparkContext.broadcast(WEATHER_CODES)
        
        @udf(StringType())
        def get_weather_description(code):
            if code is None:
                return None
            codes = weather_codes_broadcast.value
            return codes.get(int(code), "Unknown")
        
        self.get_weather_description = get_weather_description
        logger.info("✓ Registered weather description UDF")
    
    def build_abfs_path(self, file_path: str) -> str:
        """
        Build the ABFS (Azure Blob File System) path
        
        Args:
            file_path: Path to the file within the container
            
        Returns:
            Full ABFS path
        """
        return f"abfss://{self.container_name}@{self.storage_account}.dfs.core.windows.net/{file_path}"
    
    def _get_staging_path(self) -> str:
        """Get staging path from Task 1 output"""
        if self.input_path:
            return self.input_path
        
        now = datetime.now(timezone.utc)
        return f"{self.container_path}/staging/year={now.year}/month={now.month:02d}/day={now.day:02d}/weather_hourly"
    
    def read_parquet(self, file_path: str) -> DataFrame:
        """
        Read Parquet file from Azure Data Lake Storage
        
        Args:
            file_path: Path to read the Parquet file
            
        Returns:
            Spark DataFrame
        """
        abfs_path = self.build_abfs_path(file_path)
        logger.info(f"Reading Parquet from: {abfs_path}")
        
        df = self.spark.read.parquet(abfs_path)
        
        logger.info(f"✓ Parquet read successfully from {abfs_path}")
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
    
    def read_staging(self) -> DataFrame:
        """Read data from ADLS staging using Spark (distributed read)"""
        logger.info("=" * 60)
        logger.info("TASK 2: TRANSFORM & LOAD (Spark Distributed)")
        logger.info("=" * 60)
        
        staging_path = self._get_staging_path()
        
        # Read Parquet using Spark - DISTRIBUTED OPERATION
        df = self.read_parquet(staging_path)
        
        record_count = df.count()  # Action - triggers distributed read
        logger.info(f"✓ Read {record_count} records from staging using Spark")
        logger.info(f"  Schema: {df.schema.simpleString()}")
        logger.info(f"  Partitions: {df.rdd.getNumPartitions()}")
        
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform weather data using Spark DataFrame operations (distributed)
        
        Args:
            df: Input DataFrame from staging
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming weather data using Spark (distributed)...")
        
        # Rename columns for clarity - Spark transformation
        df = df.withColumnRenamed("temperature_2m", "temperature_c") \
               .withColumnRenamed("relative_humidity_2m", "humidity_percent") \
               .withColumnRenamed("precipitation", "precipitation_mm") \
               .withColumnRenamed("wind_speed_10m", "wind_speed_kmh")
        
        # Round numeric values - Spark transformation
        df = df.withColumn("temperature_c", spark_round(col("temperature_c"), 1)) \
               .withColumn("precipitation_mm", spark_round(col("precipitation_mm"), 1)) \
               .withColumn("wind_speed_kmh", spark_round(col("wind_speed_kmh"), 1))
        logger.info("  → Rounded numeric values")
        
        # Add weather description using UDF - Spark distributed operation
        df = df.withColumn("weather_description", self.get_weather_description(col("weather_code")))
        logger.info("  → Added weather descriptions using UDF")
        
        # Add ETL metadata - Spark transformation
        df = df.withColumn("_transformed_at", current_timestamp()) \
               .withColumn("_pipeline", lit("weather_etl_pipeline")) \
               .withColumn("_version", lit("3.0.0"))
        
        # Repartition for optimal write performance
        df = df.repartition(4)
        
        # Trigger computation to verify transformation
        record_count = df.count()
        logger.info(f"✓ Transform complete: {record_count} rows")
        logger.info(f"  Columns: {df.columns}")
        logger.info(f"  Partitions: {df.rdd.getNumPartitions()}")
        
        return df
    
    def run(self) -> dict:
        """Execute the transform & load task with distributed Spark processing"""
        record_count = 0
        try:
            # Step 1: Read from staging using Spark (distributed)
            df = self.read_staging()
            
            # Show sample of data
            logger.info("Sample data from staging:")
            df.show(5, truncate=False)
            
            # Step 2: Transform using Spark DataFrame operations (distributed)
            df_transformed = self.transform(df)
            record_count = df_transformed.count()
            
            # Show sample of transformed data
            logger.info("Sample transformed data:")
            df_transformed.show(5, truncate=False)
            logger.info("Schema:")
            df_transformed.printSchema()
            
            # Step 3: Load to curated using Spark (distributed)
            now = datetime.now(timezone.utc)
            curated_path = f"{self.container_path}/curated/year={now.year}/month={now.month:02d}/day={now.day:02d}/weather_hourly"
            
            self.write_parquet(df_transformed, curated_path)
            
            result = {
                "status": "success",
                "task": "transform_load",
                "output_path": curated_path,
                "container": self.container_name,
                "records": record_count,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info("=" * 60)
            logger.info("✓ TASK 2 COMPLETE: Transform & Load succeeded (Spark distributed)")
            logger.info(f"  Output: {self.container_name}/{curated_path}")
            logger.info(f"  Records: {record_count} hourly forecasts")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"Transform/Load failed: {e}")
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
    """Main function to run Weather ETL Transform & Load"""
    logger.info("=" * 60)
    logger.info("Weather ETL - Task 2 Transform & Load - Acceldata")
    logger.info("=" * 60)
    logger.info("Company: acceldata.io")
    logger.info("Configuration loaded from Kubernetes datastore secrets")
    logger.info("=" * 60)
    
    loader = WeatherTransformLoader()
    result = loader.run()
    
    # Print result as JSON for pipeline orchestration
    print(json.dumps(result))
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
