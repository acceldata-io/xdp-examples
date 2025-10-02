# S3 GPU Spark Test Jobs

## Overview

This project contains a collection of Apache Spark applications designed to test and validate S3-compatible object storage integration (particularly VAST Data) with GPU-enabled Spark clusters. The applications demonstrate various workload patterns including simple I/O operations, heavy computational jobs, benchmarking, and database operations.

## Project Structure

```
06-s3-gpu-spark/
├── build.sbt                     # SBT build configuration
├── project/
│   ├── build.properties          # SBT version configuration
│   └── plugins.sbt               # SBT plugins (assembly)
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── Dockerfile        # Docker image for Spark deployment
│   │   └── scala/
│   │       └── com/acceldata/spark/vast/
│   │           ├── CreateBucket.scala       # S3 bucket operations
│   │           ├── HeavyGpuJobVast.scala    # GPU-intensive workload
│   │           ├── SparkIOLoadTest.scala    # I/O benchmarking suite
│   │           ├── VastDBOps.scala          # Database operations
│   │           └── VastS3EnvJob.scala       # Basic S3 connectivity test
└── README.md
```

## Applications

### 1. VastS3EnvJob
**Purpose:** Basic S3 connectivity and environment validation  
**Main Class:** `com.acceldata.spark.vast.VastS3EnvJob`

A simple Spark application that:
- Validates S3 credentials from environment variables
- Checks for GPU resource availability
- Writes a sample DataFrame to S3
- Reads the data back and displays it

### 2. CreateBucket
**Purpose:** S3 bucket creation and basic operations  
**Main Class:** `com.acceldata.spark.vast.CreateBucket`

Similar to VastS3EnvJob but specifically designed for:
- Creating and testing S3 bucket operations
- Writing to specific paths (e.g., `/spark-history/`)
- Validating bucket accessibility

### 3. HeavyGpuJobVast
**Purpose:** GPU-intensive computational workload  
**Main Class:** `com.acceldata.spark.vast.HeavyGpuJobVast`

A heavy computational job that:
- Generates large synthetic datasets (200M records)
- Performs shuffle-heavy transformations
- Executes complex joins and aggregations
- Measures execution time and performance
- Dumps Spark configuration for debugging

### 4. SparkIOLoadTest
**Purpose:** Comprehensive I/O benchmarking suite  
**Main Class:** `com.acceldata.spark.vast.SparkIOLoadTest`

A parameterized benchmarking tool supporting multiple test cases:
- **write**: Sequential write of wide columns (25+ columns)
- **read**: Sequential read with row counting
- **shuffleagg**: Shuffle with aggregation operations
- **mixedetl**: Mixed ETL with repartitioning
- **randomread**: Random access pattern reads

**Usage:** Requires command-line arguments:
```bash
SparkIOLoadTest <testCase> <scaleFactor> [outputPath]
```

### 5. VastDBOps
**Purpose:** Database operations and metadata queries  
**Main Class:** `com.acceldata.spark.vast.VastDBOps`

Demonstrates:
- Database connectivity through Spark SQL
- Metadata operations (SHOW DATABASES)
- GPU resource detection

## Environment Variables

All applications read S3 configuration from environment variables:

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DATASTORE_AWS_ACCESS_KEY_ID` | S3 access key | Yes | - |
| `DATASTORE_AWS_SECRET_ACCESS_KEY` | S3 secret key | Yes | - |
| `DATASTORE_S3_BUCKET_NAME` | Target S3 bucket name | Yes | - |
| `DATASTORE_S3_ENDPOINT_URL` | S3 endpoint URL | Yes | - |
| `DATASTORE_S3_AUTHENTICATION_TYPE` | Authentication type | No | `access-key` |
| `DATASTORE_S3_REGION` | S3 region | No | `us-east-1` |

## Build Instructions

### Prerequisites
- Java 11+
- Scala 2.12.18
- SBT 1.11.6
- Apache Spark 3.5.1

### Building the JAR

1. Clone the repository:
```bash
git clone <repository-url>
cd 06-s3-gpu-spark
```

2. Build the assembly JAR:
```bash
sbt clean assembly
```

This creates a fat JAR at: `target/scala-2.12/vasts3-test-job-assembly-0.1.jar`

## Running the Applications

### Setting Environment Variables

```bash
export DATASTORE_AWS_ACCESS_KEY_ID="your-access-key"
export DATASTORE_AWS_SECRET_ACCESS_KEY="your-secret-key"
export DATASTORE_S3_BUCKET_NAME="your-bucket"
export DATASTORE_S3_ENDPOINT_URL="https://s3.vastdata.example.com"
export DATASTORE_S3_REGION="us-east-1"
```

### Example spark-submit Commands

#### Basic S3 Test:
```bash
spark-submit \
  --class com.acceldata.spark.vast.VastS3EnvJob \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar
```

#### Heavy GPU Job:
```bash
spark-submit \
  --class com.acceldata.spark.vast.HeavyGpuJobVast \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.task.resource.gpu.amount=1 \
  --conf spark.rapids.sql.enabled=true \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar
```

#### I/O Benchmark (Write Test):
```bash
spark-submit \
  --class com.acceldata.spark.vast.SparkIOLoadTest \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar \
  write 10000000
```

## Docker Deployment

The project includes a Dockerfile for containerized deployment with:
- Base image: Spark 3.5.1 with Scala 2.12, Python 3.11, JDK 11
- RAPIDS GPU acceleration libraries
- VAST DB connector JARs
- Configured for user ID 185 (Spark user)

### Building the Docker Image:
```bash
docker build -t spark-vast-gpu:latest -f src/main/resources/Dockerfile .
```

## GPU Configuration

For GPU-enabled execution, add these Spark configurations:
```bash
--conf spark.executor.resource.gpu.amount=1
--conf spark.task.resource.gpu.amount=1
--conf spark.rapids.sql.enabled=true
--conf spark.plugins=com.nvidia.spark.SQLPlugin
```

## Dependencies

### Core Dependencies (from build.sbt):
- Apache Spark Core 3.5.1 (provided)
- Apache Spark SQL 3.5.1 (provided)
- Typesafe Config 1.4.3

### Additional Runtime Dependencies:
- Hadoop AWS SDK (for S3A connector)
- RAPIDS for Spark (for GPU acceleration)
- VAST DB connector libraries

## Output Examples

### VastS3EnvJob Output:
```
GPU resources detected: executor=1, task=1
Data written to s3a://your-bucket/tmp/spark_env_test/
+---+-----+
|id |value|
+---+-----+
|1  |alpha|
|2  |beta |
|3  |gamma|
+---+-----+
Successfully wrote and read back data from VAST S3 (via env vars)
```

### SparkIOLoadTest Output:
```
Wrote 10000000 rows with 25 columns to s3a://your-bucket/tmp/spark_env_test/benchmark/seq_write
Row count = 10000000, Columns = 25
```

## Troubleshooting

### Common Issues:

1. **Missing Environment Variables:**
   - Ensure all required environment variables are set and exported
   - Check variable names match exactly (case-sensitive)

2. **S3 Connection Failures:**
   - Verify endpoint URL is correct and accessible
   - Check credentials have appropriate permissions
   - Ensure bucket exists and is accessible

3. **GPU Not Detected:**
   - Verify cluster has GPU nodes available
   - Check Spark GPU resource configurations
   - Ensure RAPIDS libraries are properly installed

4. **ClassNotFoundException:**
   - Verify the correct main class name is specified
   - Ensure the JAR was built successfully with all dependencies

5. **S3A FileSystem Issues:**
   - Ensure Hadoop AWS libraries are on the classpath
   - Verify S3A configurations are properly set

## Performance Tuning

### Recommended Configurations:
```bash
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.shuffle.partitions=200
--conf spark.hadoop.fs.s3a.connection.maximum=100
--conf spark.hadoop.fs.s3a.fast.upload=true
--conf spark.hadoop.fs.s3a.block.size=128M
```

## Contributing

When adding new Spark applications:
1. Follow the existing package structure
2. Use environment variables for configuration
3. Include GPU detection logic
4. Add comprehensive error handling
5. Update this README with the new application details

## License

Apache License 2.0 or as specified in the repository.

## Support

For issues related to:
- VAST S3 connectivity: Contact VAST Data support
- Spark configuration: Refer to Apache Spark documentation
- GPU/RAPIDS issues: Check NVIDIA RAPIDS documentation