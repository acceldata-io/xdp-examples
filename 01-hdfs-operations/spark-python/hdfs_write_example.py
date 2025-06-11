#!/usr/bin/env python3
"""
HDFS Write Operations Example

This example demonstrates various ways to write data to HDFS using PySpark:
1. Writing RDD as text files
2. Writing DataFrame as CSV
3. Writing DataFrame as JSON
4. Writing DataFrame as Parquet
5. Writing with different partitioning strategies

Author: Acceldata Platform Team
Version: 1.0.0
Company: acceldata.io
"""

import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, lit, current_timestamp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HDFSWriteExample:
    """HDFS Write Operations using PySpark"""
    
    def __init__(self, hdfs_namenode: str = "hdfs://localhost:9000"):
        """
        Initialize HDFS Write Example
        
        Args:
            hdfs_namenode: HDFS namenode URL
        """
        self.hdfs_namenode = hdfs_namenode
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        return SparkSession.builder \
            .appName("HDFS Write Example - Acceldata") \
            .master("local[*]") \
            .config("spark.hadoop.fs.defaultFS", self.hdfs_namenode) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    
    def write_rdd_as_text(self, output_path: str) -> None:
        """
        Write RDD as text file
        
        Args:
            output_path: Output path in HDFS
        """
        try:
            logger.info(f"Writing RDD as text file to: {output_path}")
            
            # Create sample data
            sample_data = [
                "Welcome to Acceldata HDFS Examples",
                "This is line 2 of our sample data",
                "PySpark makes big data processing easy",
                "HDFS provides distributed storage",
                "Data engineering with Acceldata platform"
            ]
            
            # Create RDD
            text_rdd = self.spark.sparkContext.parallelize(sample_data)
            
            # Add timestamp to each line
            timestamped_rdd = text_rdd.map(
                lambda line: f"{datetime.now().isoformat()} - {line}"
            )
            
            # Write to HDFS
            timestamped_rdd.saveAsTextFile(output_path)
            
            logger.info(f"Successfully wrote {len(sample_data)} lines to {output_path}")
            
        except Exception as e:
            logger.error(f"Error writing RDD as text: {str(e)}")
    
    def write_dataframe_as_csv(self, output_path: str) -> None:
        """
        Write DataFrame as CSV
        
        Args:
            output_path: Output path in HDFS
        """
        try:
            logger.info(f"Writing DataFrame as CSV to: {output_path}")
            
            # Create sample DataFrame
            df = self._create_sample_dataframe()
            
            # Write as CSV with options
            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .option("header", "true") \
              .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
              .csv(output_path)
            
            logger.info(f"Successfully wrote DataFrame as CSV to {output_path}")
            
        except Exception as e:
            logger.error(f"Error writing DataFrame as CSV: {str(e)}")
    
    def write_dataframe_as_json(self, output_path: str) -> None:
        """
        Write DataFrame as JSON
        
        Args:
            output_path: Output path in HDFS
        """
        try:
            logger.info(f"Writing DataFrame as JSON to: {output_path}")
            
            # Create sample DataFrame
            df = self._create_sample_dataframe()
            
            # Write as JSON
            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
              .json(output_path)
            
            logger.info(f"Successfully wrote DataFrame as JSON to {output_path}")
            
        except Exception as e:
            logger.error(f"Error writing DataFrame as JSON: {str(e)}")
    
    def write_dataframe_as_parquet(self, output_path: str) -> None:
        """
        Write DataFrame as Parquet
        
        Args:
            output_path: Output path in HDFS
        """
        try:
            logger.info(f"Writing DataFrame as Parquet to: {output_path}")
            
            # Create sample DataFrame
            df = self._create_sample_dataframe()
            
            # Write as Parquet
            df.write \
              .mode("overwrite") \
              .parquet(output_path)
            
            logger.info(f"Successfully wrote DataFrame as Parquet to {output_path}")
            
        except Exception as e:
            logger.error(f"Error writing DataFrame as Parquet: {str(e)}")
    
    def write_with_partitioning(self, output_path: str) -> None:
        """
        Write with partitioning strategy
        
        Args:
            output_path: Output path in HDFS
        """
        try:
            logger.info(f"Writing DataFrame with partitioning to: {output_path}")
            
            # Create sample DataFrame with partition columns
            df = self._create_sample_dataframe_with_partitions()
            
            # Write with partitioning
            df.write \
              .mode("overwrite") \
              .partitionBy("department", "year") \
              .parquet(output_path)
            
            logger.info(f"Successfully wrote partitioned DataFrame to {output_path}")
            
        except Exception as e:
            logger.error(f"Error writing partitioned DataFrame: {str(e)}")
    
    def write_with_compression(self, output_path: str) -> None:
        """
        Write with compression
        
        Args:
            output_path: Output path in HDFS
        """
        try:
            logger.info(f"Writing DataFrame with compression to: {output_path}")
            
            # Create sample DataFrame
            df = self._create_sample_dataframe()
            
            # Write with GZIP compression
            df.write \
              .mode("overwrite") \
              .option("compression", "gzip") \
              .parquet(output_path)
            
            logger.info(f"Successfully wrote compressed DataFrame to {output_path}")
            
        except Exception as e:
            logger.error(f"Error writing compressed DataFrame: {str(e)}")
    
    def write_streaming_data(self, output_path: str) -> None:
        """
        Simulate writing streaming data
        
        Args:
            output_path: Output path in HDFS
        """
        try:
            logger.info(f"Writing streaming data simulation to: {output_path}")
            
            # Create multiple batches of data
            for batch_id in range(3):
                logger.info(f"Writing batch {batch_id + 1}")
                
                # Create batch data
                batch_df = self._create_batch_dataframe(batch_id)
                
                # Write batch with timestamp in path
                batch_path = f"{output_path}/batch_{batch_id:03d}"
                batch_df.write \
                       .mode("overwrite") \
                       .parquet(batch_path)
                
                logger.info(f"Batch {batch_id + 1} written to {batch_path}")
            
            logger.info("Streaming data simulation completed")
            
        except Exception as e:
            logger.error(f"Error writing streaming data: {str(e)}")
    
    def _create_sample_dataframe(self):
        """Create sample DataFrame for examples"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("email", StringType(), False),
            StructField("salary", DoubleType(), False),
            StructField("created_at", TimestampType(), False)
        ])
        
        data = [
            (1, "John Doe", "john.doe@acceldata.io", 75000.0, datetime(2023, 1, 15, 10, 30, 0)),
            (2, "Jane Smith", "jane.smith@acceldata.io", 82000.0, datetime(2023, 2, 20, 14, 15, 0)),
            (3, "Bob Johnson", "bob.johnson@acceldata.io", 68000.0, datetime(2023, 3, 10, 9, 45, 0)),
            (4, "Alice Brown", "alice.brown@acceldata.io", 91000.0, datetime(2023, 4, 5, 16, 20, 0)),
            (5, "Charlie Wilson", "charlie.wilson@acceldata.io", 77500.0, datetime(2023, 5, 12, 11, 10, 0))
        ]
        
        return self.spark.createDataFrame(data, schema)
    
    def _create_sample_dataframe_with_partitions(self):
        """Create sample DataFrame with partition columns"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("department", StringType(), False),
            StructField("salary", DoubleType(), False),
            StructField("year", IntegerType(), False)
        ])
        
        departments = ["Engineering", "Sales", "Marketing", "HR"]
        years = [2022, 2023, 2024]
        
        data = [
            (1, "John Doe", departments[0], 75000.0 + random.randint(0, 20000), years[1]),
            (2, "Jane Smith", departments[1], 82000.0 + random.randint(0, 20000), years[1]),
            (3, "Bob Johnson", departments[0], 68000.0 + random.randint(0, 20000), years[2]),
            (4, "Alice Brown", departments[2], 91000.0 + random.randint(0, 20000), years[2]),
            (5, "Charlie Wilson", departments[3], 77500.0 + random.randint(0, 20000), years[1]),
            (6, "Diana Prince", departments[0], 85000.0 + random.randint(0, 20000), years[2]),
            (7, "Frank Miller", departments[1], 72000.0 + random.randint(0, 20000), years[0]),
            (8, "Grace Lee", departments[2], 88000.0 + random.randint(0, 20000), years[0])
        ]
        
        return self.spark.createDataFrame(data, schema)
    
    def _create_batch_dataframe(self, batch_id: int):
        """Create batch DataFrame for streaming simulation"""
        schema = StructType([
            StructField("batch_id", IntegerType(), False),
            StructField("record_id", IntegerType(), False),
            StructField("value", DoubleType(), False),
            StructField("timestamp", TimestampType(), False)
        ])
        
        # Generate random data for this batch
        batch_size = 100
        data = []
        base_time = datetime.now() - timedelta(hours=batch_id)
        
        for i in range(batch_size):
            data.append((
                batch_id,
                batch_id * batch_size + i,
                random.uniform(0, 1000),
                base_time + timedelta(seconds=i)
            ))
        
        return self.spark.createDataFrame(data, schema)
    
    def close(self) -> None:
        """Close Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main function to run HDFS write examples"""
    hdfs_path = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/data/output"
    
    logger.info("=== Acceldata HDFS Write Operations Example ===")
    logger.info(f"Company: acceldata.io")
    logger.info(f"Output HDFS Path: {hdfs_path}")
    
    # Initialize HDFS writer
    hdfs_writer = HDFSWriteExample()
    
    try:
        logger.info("Starting HDFS Write Operations Example")
        
        # Example 1: Write RDD as text file
        hdfs_writer.write_rdd_as_text(f"{hdfs_path}/text_output")
        
        # Example 2: Write DataFrame as CSV
        hdfs_writer.write_dataframe_as_csv(f"{hdfs_path}/csv_output")
        
        # Example 3: Write DataFrame as JSON
        hdfs_writer.write_dataframe_as_json(f"{hdfs_path}/json_output")
        
        # Example 4: Write DataFrame as Parquet
        hdfs_writer.write_dataframe_as_parquet(f"{hdfs_path}/parquet_output")
        
        # Example 5: Write with partitioning
        hdfs_writer.write_with_partitioning(f"{hdfs_path}/partitioned_output")
        
        # Example 6: Write with compression
        hdfs_writer.write_with_compression(f"{hdfs_path}/compressed_output")
        
        # Example 7: Write streaming data simulation
        hdfs_writer.write_streaming_data(f"{hdfs_path}/streaming_output")
        
        logger.info("HDFS Write Operations completed successfully")
        
    except Exception as e:
        logger.error(f"Error during HDFS write operations: {str(e)}")
        sys.exit(1)
    finally:
        hdfs_writer.close()


if __name__ == "__main__":
    main() 