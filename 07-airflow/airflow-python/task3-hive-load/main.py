#!/usr/bin/env python3
"""
Task 3: Load Transformed Data to Hive

This task reads the transformed Parquet data from ADLS curated zone
and loads it into a Hive table for analytics and querying.

Expects data from Task 2 (Transform & Load) output in curated zone.

Environment Variables (auto-injected from Kubernetes datastore secrets):
- DATASTORE_ADLS_STORAGE_ACCOUNT_NAME: Storage account name
- DATASTORE_ADLS_CONTAINER_NAME: Container/filesystem name
- DATASTORE_ADLS_CONTAINER_PATH: Path to curated data
- HIVE_DATABASE: Target Hive database (default: weather_db)
- HIVE_TABLE: Target Hive table (default: weather_hourly)
- INPUT_PATH: Optional override for input path

Author: Acceldata Platform Team
Version: 1.0.0
Company: acceldata.io
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from pyspark.sql import SparkSession

KERBEROS_PRINCIPAL = os.environ.get("KERBEROS_PRINCIPAL", "hdfs-adocqecluster@ADSRE.COM")
KERBEROS_KEYTAB = os.environ.get("KERBEROS_KEYTAB", "/etc/user.keytab")
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HiveLoader:
    """Load transformed weather data from ADLS to Hive"""
    
    def __init__(self):
        """
        Initialize Hive Loader
        
        All Azure configuration is automatically picked from Kubernetes datastore secrets.
        """
        self.storage_account = os.environ.get("DATASTORE_ADLS_STORAGE_ACCOUNT_NAME")
        self.container_name = os.environ.get("DATASTORE_ADLS_CONTAINER_NAME")
        self.container_path = os.environ.get("DATASTORE_ADLS_CONTAINER_PATH")
        
        # Hive configuration
        self.hive_database = os.environ.get("HIVE_DATABASE", "weather_db")
        self.hive_table = os.environ.get("HIVE_TABLE", "weather_hourly")
        
        # Optional: override input path
        self.input_path = os.environ.get("INPUT_PATH")
        
        # Initialize Spark session with Hive support
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session with Hive support and Kerberos auth"""
        logger.info("Creating Spark session with Hive support...")
        
        spark = SparkSession.builder \
            .appName("Weather ETL - Task 3 Hive Load - Acceldata") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .enableHiveSupport() \
            .getOrCreate()
        
        # Setup Kerberos authentication for Hive
        logger.info("Setting up Kerberos authentication...")
        logger.info(f"  Principal: {KERBEROS_PRINCIPAL}")
        logger.info(f"  Keytab: {KERBEROS_KEYTAB}")
        
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("hadoop.security.authentication", "kerberos")
        hadoop_conf.set("hadoop.security.authorization", "true")
        
        try:
            UserGroupInformation = spark.sparkContext._jvm.org.apache.hadoop.security.UserGroupInformation
            UserGroupInformation.setConfiguration(hadoop_conf)
            UserGroupInformation.loginUserFromKeytab(KERBEROS_PRINCIPAL, KERBEROS_KEYTAB)
            logger.info(f"✓ Kerberos login successful as: {KERBEROS_PRINCIPAL}")
        except Exception as e:
            logger.error(f"Kerberos authentication failed: {e}")
            raise
        
        logger.info("✓ Spark session created with Hive + Kerberos support")
        return spark
    
    def build_abfs_path(self, file_path: str) -> str:
        """Build the ABFS (Azure Blob File System) path"""
        return f"abfss://{self.container_name}@{self.storage_account}.dfs.core.windows.net/{file_path}"
    
    def _get_curated_path(self) -> str:
        """Get curated path from Task 2 output"""
        if self.input_path:
            return self.input_path
        
        now = datetime.now(timezone.utc)
        return f"{self.container_path}/curated/year={now.year}/month={now.month:02d}/day={now.day:02d}/weather_hourly"
    
    def read_curated_data(self):
        """Read transformed data from ADLS curated zone"""
        logger.info("=" * 60)
        logger.info("TASK 3: HIVE LOAD")
        logger.info("=" * 60)
        
        curated_path = self._get_curated_path()
        abfs_path = self.build_abfs_path(curated_path)
        
        logger.info(f"Reading from curated zone: {abfs_path}")
        
        df = self.spark.read.parquet(abfs_path)
        
        record_count = df.count()
        logger.info(f"✓ Read {record_count} records from curated zone")
        logger.info(f"  Schema: {df.schema.simpleString()}")
        
        return df
    
    def create_hive_database(self):
        """Create Hive database if not exists"""
        logger.info(f"Creating Hive database: {self.hive_database}")
        
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.hive_database}")
        self.spark.sql(f"USE {self.hive_database}")
        
        logger.info(f"✓ Using database: {self.hive_database}")
    
    def write_to_hive(self, df) -> str:
        """Write DataFrame to Hive table"""
        full_table_name = f"{self.hive_database}.{self.hive_table}"
        logger.info(f"Writing to Hive table: {full_table_name}")
        
        # Show sample data
        logger.info("Sample data to be written to Hive:")
        df.show(5, truncate=False)
        
        # Write to Hive with overwrite mode
        # Using saveAsTable for managed table or insertInto for existing table
        df.write \
            .mode("overwrite") \
            .format("hive") \
            .option("compression", "snappy") \
            .saveAsTable(full_table_name)
        
        logger.info(f"✓ Data written to Hive table: {full_table_name}")
        
        # Verify the write
        count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]["cnt"]
        logger.info(f"✓ Verified: {count} records in Hive table")
        
        # Show table info
        logger.info("Table schema:")
        self.spark.sql(f"DESCRIBE {full_table_name}").show(truncate=False)
        
        return full_table_name
    
    def run(self) -> dict:
        """Execute the Hive load task"""
        try:
            # Step 1: Read from curated zone
            df = self.read_curated_data()
            
            # Step 2: Create Hive database
            self.create_hive_database()
            
            # Step 3: Write to Hive
            table_name = self.write_to_hive(df)
            
            result = {
                "status": "success",
                "task": "hive_load",
                "hive_database": self.hive_database,
                "hive_table": self.hive_table,
                "full_table_name": table_name,
                "records": df.count(),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info("=" * 60)
            logger.info("✓ TASK 3 COMPLETE: Hive Load succeeded")
            logger.info(f"  Table: {table_name}")
            logger.info(f"  Records: {df.count()}")
            logger.info("=" * 60)
            
            return result
            
        except Exception as e:
            logger.error(f"Hive load failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("Weather ETL - Task 3 Hive Load - Acceldata")
    logger.info("=" * 60)
    logger.info("Company: acceldata.io")
    logger.info("Configuration loaded from Kubernetes datastore secrets")
    logger.info("=" * 60)
    
    loader = HiveLoader()
    result = loader.run()
    
    # Print result as JSON for pipeline orchestration
    print(json.dumps(result))
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

