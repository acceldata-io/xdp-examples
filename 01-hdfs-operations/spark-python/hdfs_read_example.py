#!/usr/bin/env python3
"""
HDFS Read Operations Example

This example demonstrates various ways to read data from HDFS using PySpark:
1. Reading text files as RDD
2. Reading structured data as DataFrame
3. Reading JSON files
4. Reading Parquet files
5. Reading multiple files with pattern matching

Author: Acceldata Platform Team
Version: 1.0.0
Company: acceldata.io
"""

import sys
import logging
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HDFSReadExample:
    """HDFS Read Operations using PySpark"""
    
    def __init__(self, hdfs_namenode: str = "hdfs://localhost:9000"):
        """
        Initialize HDFS Read Example
        
        Args:
            hdfs_namenode: HDFS namenode URL
        """
        self.hdfs_namenode = hdfs_namenode
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        return SparkSession.builder \
            .appName("HDFS Read Example - Acceldata") \
            .master("local[*]") \
            .config("spark.hadoop.fs.defaultFS", self.hdfs_namenode) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    
    def read_text_file_as_rdd(self, file_path: str) -> None:
        """
        Read text file as RDD and perform basic operations
        
        Args:
            file_path: Path to the text file in HDFS
        """
        try:
            logger.info(f"Reading text file as RDD: {file_path}")
            
            # Read text file as RDD
            text_rdd = self.spark.sparkContext.textFile(file_path)
            
            # Basic RDD operations
            line_count = text_rdd.count()
            logger.info(f"Total lines in file: {line_count}")
            
            # Filter non-empty lines
            non_empty_lines = text_rdd.filter(lambda line: line.strip() != "")
            logger.info(f"Non-empty lines: {non_empty_lines.count()}")
            
            # Show first 5 lines
            logger.info("First 5 lines:")
            for line in non_empty_lines.take(5):
                logger.info(f"  {line}")
            
            # Word count example
            words = text_rdd.flatMap(lambda line: line.split())
            word_count = words.count()
            logger.info(f"Total words: {word_count}")
            
            # Most common words
            word_counts = words.map(lambda word: (word.lower(), 1)) \
                              .reduceByKey(lambda a, b: a + b) \
                              .sortBy(lambda x: x[1], ascending=False)
            
            logger.info("Top 5 most common words:")
            for word, count in word_counts.take(5):
                logger.info(f"  {word}: {count}")
                
        except Exception as e:
            logger.warning(f"Could not read text file: {file_path} - {str(e)}")
    
    def read_csv_as_dataframe(self, file_path: str) -> None:
        """
        Read CSV file as DataFrame
        
        Args:
            file_path: Path to the CSV file in HDFS
        """
        try:
            logger.info(f"Reading CSV file as DataFrame: {file_path}")
            
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                .csv(file_path)
            
            # Show schema
            logger.info("CSV Schema:")
            df.printSchema()
            
            # Show data
            logger.info("CSV Data (first 10 rows):")
            df.show(10)
            
            # Basic statistics
            logger.info(f"Row count: {df.count()}")
            logger.info(f"Column count: {len(df.columns)}")
            
            # Numeric columns statistics
            numeric_cols = [field.name for field in df.schema.fields 
                          if field.dataType in [IntegerType(), DoubleType()]]
            
            if numeric_cols:
                logger.info("Numeric columns statistics:")
                df.select(numeric_cols).describe().show()
                
        except Exception as e:
            logger.warning(f"Could not read CSV file: {file_path} - {str(e)}")
    
    def read_json_as_dataframe(self, file_path: str) -> None:
        """
        Read JSON file as DataFrame
        
        Args:
            file_path: Path to the JSON file in HDFS
        """
        try:
            logger.info(f"Reading JSON file as DataFrame: {file_path}")
            
            df = self.spark.read \
                .option("multiline", "true") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                .json(file_path)
            
            # Show schema
            logger.info("JSON Schema:")
            df.printSchema()
            
            # Show data
            logger.info("JSON Data (first 10 rows):")
            df.show(10, truncate=False)
            
            # Basic statistics
            logger.info(f"Row count: {df.count()}")
            
        except Exception as e:
            logger.warning(f"Could not read JSON file: {file_path} - {str(e)}")
    
    def read_parquet_as_dataframe(self, file_path: str) -> None:
        """
        Read Parquet file as DataFrame
        
        Args:
            file_path: Path to the Parquet file in HDFS
        """
        try:
            logger.info(f"Reading Parquet file as DataFrame: {file_path}")
            
            df = self.spark.read.parquet(file_path)
            
            # Show schema
            logger.info("Parquet Schema:")
            df.printSchema()
            
            # Show data
            logger.info("Parquet Data (first 10 rows):")
            df.show(10)
            
            # Basic statistics
            logger.info(f"Row count: {df.count()}")
            logger.info(f"Partitions: {df.rdd.getNumPartitions()}")
            
        except Exception as e:
            logger.warning(f"Could not read Parquet file: {file_path} - {str(e)}")
    
    def read_multiple_files(self, path_pattern: str) -> None:
        """
        Read multiple files using pattern matching
        
        Args:
            path_pattern: Path pattern for multiple files
        """
        try:
            logger.info(f"Reading multiple files with pattern: {path_pattern}")
            
            text_rdd = self.spark.sparkContext.textFile(path_pattern)
            
            total_lines = text_rdd.count()
            logger.info(f"Total lines across all matching files: {total_lines}")
            
            # Show sample data
            if total_lines > 0:
                logger.info("Sample lines from multiple files:")
                for line in text_rdd.take(5):
                    logger.info(f"  {line}")
                    
        except Exception as e:
            logger.warning(f"Could not read files with pattern: {path_pattern} - {str(e)}")
    
    def analyze_data(self, file_path: str) -> None:
        """
        Perform data analysis on HDFS data
        
        Args:
            file_path: Path to the data file in HDFS
        """
        try:
            logger.info(f"Analyzing data from: {file_path}")
            
            # Try to read as different formats
            formats_to_try = [
                ("parquet", lambda: self.spark.read.parquet(file_path)),
                ("json", lambda: self.spark.read.json(file_path)),
                ("csv", lambda: self.spark.read.option("header", "true").option("inferSchema", "true").csv(file_path))
            ]
            
            df = None
            for format_name, read_func in formats_to_try:
                try:
                    df = read_func()
                    logger.info(f"Successfully read data as {format_name}")
                    break
                except:
                    continue
            
            if df is None:
                logger.warning("Could not read data in any supported format")
                return
            
            # Basic analysis
            logger.info("=== Data Analysis Results ===")
            logger.info(f"Total records: {df.count()}")
            logger.info(f"Total columns: {len(df.columns)}")
            
            # Column analysis
            logger.info("Column information:")
            for field in df.schema.fields:
                logger.info(f"  {field.name}: {field.dataType}")
            
            # Null value analysis
            logger.info("Null value counts:")
            null_counts = df.select([count(col(c)).alias(c) for c in df.columns])
            null_counts.show()
            
        except Exception as e:
            logger.error(f"Error during data analysis: {str(e)}")
    
    def close(self) -> None:
        """Close Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main function to run HDFS read examples"""
    hdfs_path = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/data"
    
    logger.info("=== Acceldata HDFS Read Operations Example ===")
    logger.info(f"Company: acceldata.io")
    logger.info(f"HDFS Path: {hdfs_path}")
    
    # Initialize HDFS reader
    hdfs_reader = HDFSReadExample()
    
    try:
        logger.info("Starting HDFS Read Operations Example")
        
        # Example 1: Read text file as RDD
        hdfs_reader.read_text_file_as_rdd(f"{hdfs_path}/sample.txt")
        
        # Example 2: Read CSV file as DataFrame
        hdfs_reader.read_csv_as_dataframe(f"{hdfs_path}/sample.csv")
        
        # Example 3: Read JSON file as DataFrame
        hdfs_reader.read_json_as_dataframe(f"{hdfs_path}/sample.json")
        
        # Example 4: Read Parquet file as DataFrame
        hdfs_reader.read_parquet_as_dataframe(f"{hdfs_path}/sample.parquet")
        
        # Example 5: Read multiple files with pattern matching
        hdfs_reader.read_multiple_files(f"{hdfs_path}/logs/*.log")
        
        # Example 6: Data analysis
        hdfs_reader.analyze_data(f"{hdfs_path}/sample.parquet")
        
        logger.info("HDFS Read Operations completed successfully")
        
    except Exception as e:
        logger.error(f"Error during HDFS read operations: {str(e)}")
        sys.exit(1)
    finally:
        hdfs_reader.close()


if __name__ == "__main__":
    main() 