#!/usr/bin/env python3
"""
Comprehensive HDFS Operations Example

This is the main entry point that demonstrates both read and write operations
to HDFS using PySpark. It combines examples from hdfs_read_example and
hdfs_write_example to show a complete workflow.

Author: Acceldata Platform Team
Version: 1.0.0
Company: acceldata.io
"""

import sys
import logging
from hdfs_read_example import HDFSReadExample
from hdfs_write_example import HDFSWriteExample

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HDFSOperationsExample:
    """Comprehensive HDFS Operations using PySpark"""
    
    def __init__(self, hdfs_namenode: str = "hdfs://localhost:9000"):
        """
        Initialize HDFS Operations Example
        
        Args:
            hdfs_namenode: HDFS namenode URL
        """
        self.hdfs_namenode = hdfs_namenode
        self.reader = HDFSReadExample(hdfs_namenode)
        self.writer = HDFSWriteExample(hdfs_namenode)
    
    def run_write_operations(self, base_path: str) -> None:
        """
        Run write operations to demonstrate data ingestion
        
        Args:
            base_path: Base path in HDFS for output
        """
        try:
            logger.info("Running write operations...")
            
            output_path = f"{base_path}/output"
            
            # Write sample data in different formats
            self.writer.write_rdd_as_text(f"{output_path}/text_output")
            self.writer.write_dataframe_as_csv(f"{output_path}/csv_output")
            self.writer.write_dataframe_as_json(f"{output_path}/json_output")
            self.writer.write_dataframe_as_parquet(f"{output_path}/parquet_output")
            self.writer.write_with_partitioning(f"{output_path}/partitioned_output")
            self.writer.write_with_compression(f"{output_path}/compressed_output")
            
            logger.info("✓ Sample data written to HDFS")
            
        except Exception as e:
            logger.error(f"Error in write operations: {str(e)}")
            raise e
    
    def run_read_operations(self, base_path: str) -> None:
        """
        Run read operations to demonstrate data consumption
        
        Args:
            base_path: Base path in HDFS for input
        """
        try:
            logger.info("Running read operations...")
            
            input_path = f"{base_path}/output"
            
            # Read data in different formats
            self.reader.read_text_file_as_rdd(f"{input_path}/text_output")
            self.reader.read_csv_as_dataframe(f"{input_path}/csv_output")
            self.reader.read_json_as_dataframe(f"{input_path}/json_output")
            self.reader.read_parquet_as_dataframe(f"{input_path}/parquet_output")
            
            logger.info("✓ Data read from HDFS")
            
        except Exception as e:
            logger.error(f"Error in read operations: {str(e)}")
            raise e
    
    def run_data_analysis(self, base_path: str) -> None:
        """
        Demonstrate data analysis capabilities
        
        Args:
            base_path: Base path in HDFS
        """
        try:
            logger.info("Performing data analysis on HDFS data...")
            
            # Analyze the parquet data we wrote
            parquet_path = f"{base_path}/output/parquet_output"
            self.reader.analyze_data(parquet_path)
            
            logger.info("✓ Data quality checks completed")
            logger.info("✓ Statistical analysis performed")
            logger.info("✓ Data aggregations computed")
            logger.info("✓ Results available for further processing")
            
        except Exception as e:
            logger.error(f"Error in data analysis: {str(e)}")
            raise e
    
    def run_end_to_end_pipeline(self, base_path: str) -> None:
        """
        Run a complete end-to-end data pipeline
        
        Args:
            base_path: Base path in HDFS
        """
        try:
            logger.info("=== Running End-to-End Data Pipeline ===")
            
            # Step 1: Data Ingestion
            logger.info("Step 1: Data Ingestion")
            self.run_write_operations(base_path)
            
            # Step 2: Data Consumption
            logger.info("Step 2: Data Consumption")
            self.run_read_operations(base_path)
            
            # Step 3: Data Analysis
            logger.info("Step 3: Data Analysis")
            self.run_data_analysis(base_path)
            
            # Step 4: Data Transformation (example)
            logger.info("Step 4: Data Transformation")
            self._run_data_transformation(base_path)
            
            logger.info("=== End-to-End Pipeline Completed Successfully ===")
            
        except Exception as e:
            logger.error(f"Error in end-to-end pipeline: {str(e)}")
            raise e
    
    def _run_data_transformation(self, base_path: str) -> None:
        """
        Example data transformation workflow
        
        Args:
            base_path: Base path in HDFS
        """
        try:
            logger.info("Running data transformation...")
            
            # Read the parquet data
            input_path = f"{base_path}/output/parquet_output"
            df = self.reader.spark.read.parquet(input_path)
            
            # Perform transformations
            from pyspark.sql.functions import col, when, avg, count
            
            # Add derived columns
            transformed_df = df.withColumn(
                "salary_category",
                when(col("salary") < 70000, "Low")
                .when(col("salary") < 85000, "Medium")
                .otherwise("High")
            )
            
            # Calculate aggregations
            summary_df = transformed_df.groupBy("salary_category") \
                                    .agg(
                                        count("*").alias("count"),
                                        avg("salary").alias("avg_salary")
                                    )
            
            # Write transformed data
            output_path = f"{base_path}/output/transformed_output"
            transformed_df.write.mode("overwrite").parquet(output_path)
            
            # Write summary data
            summary_path = f"{base_path}/output/summary_output"
            summary_df.write.mode("overwrite").parquet(summary_path)
            
            logger.info("✓ Data transformation completed")
            logger.info("✓ Transformed data written to HDFS")
            
        except Exception as e:
            logger.error(f"Error in data transformation: {str(e)}")
    
    def close(self) -> None:
        """Close all resources"""
        if self.reader:
            self.reader.close()
        if self.writer:
            self.writer.close()
        logger.info("All resources closed")


def main():
    """Main function to run comprehensive HDFS operations"""
    logger.info("=== Acceldata HDFS Operations Example ===")
    logger.info("Company: acceldata.io")
    logger.info("Group: io.acceldata")
    
    # Parse command line arguments
    hdfs_namenode = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000"
    base_path = sys.argv[2] if len(sys.argv) > 2 else "/data/acceldata-examples"
    
    logger.info(f"HDFS NameNode: {hdfs_namenode}")
    logger.info(f"Base Path: {base_path}")
    
    # Initialize HDFS operations
    hdfs_ops = HDFSOperationsExample(hdfs_namenode)
    
    try:
        logger.info("Starting Comprehensive HDFS Operations Example")
        
        # Run the complete pipeline
        hdfs_ops.run_end_to_end_pipeline(base_path)
        
        logger.info("Comprehensive HDFS Operations completed successfully")
        
    except Exception as e:
        logger.error(f"Error during HDFS operations: {str(e)}")
        sys.exit(1)
    finally:
        hdfs_ops.close()


if __name__ == "__main__":
    main() 