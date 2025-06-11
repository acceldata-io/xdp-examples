# Spark Python HDFS Operations Examples

This project demonstrates HDFS read and write operations using PySpark. It's part of the Acceldata data platform examples.

## Project Information

- **Company**: Acceldata Inc. (acceldata.io)
- **Version**: 1.0.0
- **Python Version**: 3.8+

## Prerequisites

- Python 3.8 or higher
- Apache Spark 3.5.0
- Hadoop 3.3.4
- HDFS cluster (or local HDFS setup)

## Installation

### 1. Create Virtual Environment (Recommended)

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

## Project Structure

```
spark-python/
├── requirements.txt              # Python dependencies
├── hdfs_read_example.py         # HDFS read operations
├── hdfs_write_example.py        # HDFS write operations
├── hdfs_operations_example.py   # Comprehensive example
└── README.md                    # This file
```

## Examples Included

### 1. hdfs_read_example.py
Demonstrates various ways to read data from HDFS:
- Reading text files as RDD
- Reading CSV files as DataFrame
- Reading JSON files as DataFrame
- Reading Parquet files as DataFrame
- Reading multiple files with pattern matching
- Data analysis capabilities

### 2. hdfs_write_example.py
Demonstrates various ways to write data to HDFS:
- Writing RDD as text files
- Writing DataFrame as CSV
- Writing DataFrame as JSON
- Writing DataFrame as Parquet
- Writing with partitioning strategies
- Writing with compression
- Streaming data simulation

### 3. hdfs_operations_example.py
Comprehensive example combining read and write operations in a complete end-to-end pipeline.

## Running the Examples

### 1. Run HDFS Read Example

```bash
# Basic usage
python hdfs_read_example.py

# With custom HDFS path
python hdfs_read_example.py hdfs://your-namenode:9000/your/data/path
```

### 2. Run HDFS Write Example

```bash
# Basic usage
python hdfs_write_example.py

# With custom HDFS path
python hdfs_write_example.py hdfs://your-namenode:9000/your/output/path
```

### 3. Run Comprehensive Operations Example

```bash
# Basic usage
python hdfs_operations_example.py

# With custom parameters
python hdfs_operations_example.py hdfs://your-namenode:9000 /data/acceldata-examples
```

## Spark Submit Examples

### Local Mode

```bash
spark-submit \
  --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  hdfs_operations_example.py \
  hdfs://localhost:9000 \
  /data/acceldata-examples
```

### Cluster Mode

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 2g \
  --executor-cores 2 \
  --driver-memory 1g \
  --conf spark.hadoop.fs.defaultFS=hdfs://your-namenode:9000 \
  hdfs_operations_example.py \
  hdfs://your-namenode:9000 \
  /data/acceldata-examples
```

### With Additional Python Files

```bash
spark-submit \
  --master local[*] \
  --py-files hdfs_read_example.py,hdfs_write_example.py \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  hdfs_operations_example.py
```

## Configuration

### HDFS Configuration

Ensure your HDFS cluster is running and accessible. Update the HDFS namenode URL in the examples:

```python
hdfs_namenode = "hdfs://your-namenode:9000"
```

### Spark Configuration

The examples include optimized Spark configurations:

```python
spark = SparkSession.builder \
    .appName("HDFS Operations Example - Acceldata") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
```

## Sample Data Formats

### CSV Format
```csv
id,name,email,salary,created_at
1,John Doe,john.doe@acceldata.io,75000.0,2023-01-15 10:30:00
2,Jane Smith,jane.smith@acceldata.io,82000.0,2023-02-20 14:15:00
```

### JSON Format
```json
{
  "id": 1,
  "name": "John Doe",
  "email": "john.doe@acceldata.io",
  "salary": 75000.0,
  "created_at": "2023-01-15T10:30:00"
}
```

## Class Structure

### HDFSReadExample Class

```python
class HDFSReadExample:
    def __init__(self, hdfs_namenode: str = "hdfs://localhost:9000")
    def read_text_file_as_rdd(self, file_path: str) -> None
    def read_csv_as_dataframe(self, file_path: str) -> None
    def read_json_as_dataframe(self, file_path: str) -> None
    def read_parquet_as_dataframe(self, file_path: str) -> None
    def read_multiple_files(self, path_pattern: str) -> None
    def analyze_data(self, file_path: str) -> None
    def close(self) -> None
```

### HDFSWriteExample Class

```python
class HDFSWriteExample:
    def __init__(self, hdfs_namenode: str = "hdfs://localhost:9000")
    def write_rdd_as_text(self, output_path: str) -> None
    def write_dataframe_as_csv(self, output_path: str) -> None
    def write_dataframe_as_json(self, output_path: str) -> None
    def write_dataframe_as_parquet(self, output_path: str) -> None
    def write_with_partitioning(self, output_path: str) -> None
    def write_with_compression(self, output_path: str) -> None
    def write_streaming_data(self, output_path: str) -> None
    def close(self) -> None
```

## Environment Variables

You can set environment variables for configuration:

```bash
export SPARK_HOME=/path/to/spark
export HADOOP_HOME=/path/to/hadoop
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
```

## Troubleshooting

### Common Issues

1. **HDFS Connection Issues**
   ```bash
   # Check HDFS status
   hdfs dfsadmin -report
   
   # Test HDFS connectivity
   hdfs dfs -ls /
   ```

2. **Python Path Issues**
   ```bash
   # Set PYTHONPATH
   export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH
   ```

3. **Memory Issues**
   ```bash
   # Increase driver memory
   export PYSPARK_DRIVER_PYTHON_OPTS="--driver-memory 2g"
   ```

4. **Import Issues**
   ```bash
   # Install findspark to locate Spark
   pip install findspark
   
   # Add to your Python script
   import findspark
   findspark.init()
   ```

### Logging Configuration

To enable detailed logging, create a `log4j.properties` file:

```properties
log4j.rootLogger=INFO, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
```

## Performance Tips

1. **Partitioning**: Use appropriate partitioning for large datasets
2. **Caching**: Cache frequently accessed DataFrames
3. **Compression**: Use compression for storage efficiency
4. **Coalescing**: Use coalesce() to reduce small files

```python
# Example optimizations
df.repartition(4, "department")  # Repartition by column
df.cache()  # Cache DataFrame
df.coalesce(1).write.parquet(path)  # Reduce output files
```

## Testing

To test the examples with sample data:

```bash
# Create test data in HDFS
hdfs dfs -mkdir -p /data/test
echo "Sample text data" | hdfs dfs -put - /data/test/sample.txt

# Run the examples
python hdfs_read_example.py hdfs://localhost:9000/data/test
```

## Contributing

This project is part of the Acceldata data platform examples. For contributions:

1. Follow PEP 8 style guidelines
2. Add type hints where appropriate
3. Include comprehensive error handling
4. Add logging for debugging
5. Test with different HDFS configurations

## License

Copyright © 2024 Acceldata Inc. All rights reserved.

## Support

For support and questions:
- Visit: https://acceldata.io
- Email: support@acceldata.io 