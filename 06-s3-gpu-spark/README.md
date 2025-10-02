# VAST S3 GPU Spark Test Suite

## Overview

This project is a comprehensive Scala Spark application suite designed for testing and validating VAST S3-compatible object store integration with Apache Spark, including GPU resource detection and performance benchmarking. The suite contains multiple specialized Spark jobs for different testing scenarios, from basic S3 connectivity validation to heavy GPU workloads and I/O performance testing.

## Purpose

This project is intended to:

- **Validate S3 Integration**: Test S3 connectivity and operations with VAST or other S3-compatible storage systems
- **GPU Resource Detection**: Verify GPU resource availability and configuration in Spark clusters
- **Performance Benchmarking**: Conduct comprehensive I/O load testing and performance analysis
- **Database Operations**: Test Spark SQL operations and database connectivity
- **Container Deployment**: Provide Docker-based deployment for consistent testing environments
- **Environment Configuration**: Demonstrate configuration management via environment variables

## Application Components

### 1. VastS3EnvJob
**Purpose**: Basic S3 connectivity validation and GPU resource detection
- Reads S3 credentials and connection details from environment variables
- Configures Spark to use the S3A connector with the supplied credentials
- Creates and writes a sample DataFrame to S3, then reads it back and displays the data
- Checks for GPU resources in the Spark runtime configuration and prints GPU/CPU usage status

### 2. HeavyGpuJobVast
**Purpose**: GPU-intensive workload testing with large datasets
- Generates synthetic datasets with 200 million records
- Performs shuffle-heavy transformations and complex aggregations
- Includes self-joins and multiple groupBy operations
- Measures execution time and provides detailed query plan analysis
- Dumps complete Spark configuration for debugging

### 3. SparkIOLoadTest
**Purpose**: Comprehensive I/O performance benchmarking
- **Test Cases**:
  - `write`: Sequential write performance testing
  - `read`: Sequential read performance testing  
  - `shuffleagg`: Shuffle and aggregation performance
  - `mixedetl`: Mixed ETL workload testing
  - `randomread`: Random read pattern testing
- Generates wide datasets with 25+ columns of various data types
- Configurable scale factors for different load levels
- Supports custom output paths

### 4. CreateBucket
**Purpose**: S3 bucket creation and basic validation
- Similar to VastS3EnvJob but writes to `spark-history/` path
- Validates bucket accessibility and basic read/write operations
- Includes GPU resource detection

### 5. VastDBOps
**Purpose**: Database operations and SQL testing
- Tests Spark SQL functionality with `SHOW DATABASES` command
- Validates database connectivity in Spark environment
- Includes standard S3 configuration and GPU detection

### Environment Variables Used

The following environment variables must be set:

- `DATASTORE_AWS_ACCESS_KEY_ID`: S3 access key.
- `DATASTORE_AWS_SECRET_ACCESS_KEY`: S3 secret key.
- `DATASTORE_S3_BUCKET_NAME`: Target S3 bucket name.
- `DATASTORE_S3_ENDPOINT_URL`: S3 endpoint URL (e.g., for VAST or MinIO).

Optional variables:

- `DATASTORE_S3_AUTHENTICATION_TYPE`: Authentication type (default: `access-key`).
- `DATASTORE_S3_REGION`: S3 region (default: `us-east-1`).

If any of the required variables are missing, the job will fail with an error.

## Prerequisites

- **Apache Spark 3.x** (with Hadoop 3.x and S3A connector)
- **Scala 2.12+**
- **S3-compatible object store** (VAST, AWS S3, MinIO, etc.)
- **Java 8+**
- The following environment variables set in your environment (see above).

## Project Structure

```
06-s3-gpu-spark/
├── build.sbt                          # SBT build configuration
├── project/
│   ├── build.properties              # SBT version (1.11.6)
│   └── plugins.sbt                   # SBT assembly plugin
├── src/main/
│   ├── resources/
│   │   └── Dockerfile                # Container deployment configuration
│   └── scala/com/acceldata/spark/vast/
│       ├── VastS3EnvJob.scala        # Basic S3 connectivity test
│       ├── HeavyGpuJobVast.scala     # GPU-intensive workload test
│       ├── SparkIOLoadTest.scala     # I/O performance benchmarking
│       ├── CreateBucket.scala        # S3 bucket creation test
│       └── VastDBOps.scala           # Database operations test
└── README.md                         # This file
```

## Build Steps

1. **Clone the repository:**
   ```sh
   git clone <repo-url>
   cd 06-s3-gpu-spark
   ```

2. **Build the fat JAR:**
   ```sh
   sbt assembly
   ```
   This will produce a JAR file: `target/scala-2.12/vasts3-test-job-assembly-0.1.jar`

## Docker Deployment

The project includes a Dockerfile for containerized deployment:

- **Base Image**: Spark 3.5.1 with Scala 2.12, Python 3.11, and JDK 11
- **GPU Support**: Includes RAPIDS for Spark JAR for GPU acceleration
- **VAST Integration**: Pre-configured with VAST database JARs
- **User Configuration**: Runs as user 185 for security compliance

## Running the Job

Set the required environment variables before running the job. Example:

```sh
export DATASTORE_AWS_ACCESS_KEY_ID=your-access-key
export DATASTORE_AWS_SECRET_ACCESS_KEY=your-secret-key
export DATASTORE_S3_BUCKET_NAME=your-bucket
export DATASTORE_S3_ENDPOINT_URL=https://s3.vastdata.example.com
export DATASTORE_S3_REGION=us-east-1
```

## Running Different Applications

### 1. Basic S3 Connectivity Test (VastS3EnvJob)
```sh
spark-submit \
  --class com.acceldata.spark.vast.VastS3EnvJob \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar
```

### 2. Heavy GPU Workload Test (HeavyGpuJobVast)
```sh
spark-submit \
  --class com.acceldata.spark.vast.HeavyGpuJobVast \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.task.resource.gpu.amount=0.25 \
  --conf spark.rapids.sql.enabled=true \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar
```

### 3. I/O Performance Benchmarking (SparkIOLoadTest)
```sh
# Sequential Write Test
spark-submit \
  --class com.acceldata.spark.vast.SparkIOLoadTest \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar \
  write 10000000

# Sequential Read Test  
spark-submit \
  --class com.acceldata.spark.vast.SparkIOLoadTest \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar \
  read 10000000

# Shuffle Aggregation Test
spark-submit \
  --class com.acceldata.spark.vast.SparkIOLoadTest \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar \
  shuffleagg 5000000
```

### 4. Database Operations Test (VastDBOps)
```sh
spark-submit \
  --class com.acceldata.spark.vast.VastDBOps \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar
```

### 5. Bucket Creation Test (CreateBucket)
```sh
spark-submit \
  --class com.acceldata.spark.vast.CreateBucket \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar
```

**Notes:**
- Adjust the JAR path as per your build output
- Add GPU-specific configurations for GPU-enabled clusters
- SparkIOLoadTest requires command-line arguments: `<testCase> <scaleFactor> [outputPath]`
- All other applications use only environment variables for configuration

## Expected Output

### VastS3EnvJob & CreateBucket & VastDBOps
- GPU resource detection status
- Sample DataFrame write/read operations to S3
- Success confirmation messages

### HeavyGpuJobVast
- Complete Spark configuration dump
- Dataset generation and processing status (200M records)
- Query execution plan details
- Performance metrics (execution time, result count)
- Sample output rows

### SparkIOLoadTest
- Test case execution confirmation
- Dataset size and column count information
- Performance metrics for each test case
- Write/read operation status

### VastDBOps
- GPU resource detection status
- Database listing output (`SHOW DATABASES`)
- Success confirmation

## Technical Specifications

### Dependencies
- **Apache Spark**: 3.5.1 (provided scope)
- **Scala**: 2.12.18
- **SBT**: 1.11.6
- **SBT Assembly Plugin**: 2.2.0
- **Typesafe Config**: 1.4.3

### GPU Support
- **RAPIDS for Spark**: 25.06.0.3.3.6.2-1-cuda11
- GPU resource detection and configuration
- CUDA 11 compatibility

### Container Features
- **Base Image**: Spark 3.5.1 with JDK 11 on Ubuntu 24.04
- **Security**: Non-root user execution (UID 185)
- **VAST Integration**: Pre-installed VAST database JARs
- **Permissions**: Optimized for Kubernetes/container environments

## Performance Considerations

### SparkIOLoadTest Scale Factors
- **Small**: 1,000,000 records
- **Medium**: 10,000,000 records  
- **Large**: 100,000,000 records
- **Extra Large**: 1,000,000,000+ records

### HeavyGpuJobVast Workload
- **Dataset Size**: 200 million records
- **Partitions**: 200 (configurable via repartition)
- **Operations**: Self-joins, aggregations, complex transformations
- **Memory**: Requires substantial executor memory for large datasets

## Troubleshooting

### Common Issues
- **Environment Variables**: Ensure all required S3 environment variables are set and exported
- **S3 Permissions**: Verify S3 endpoint and credentials have appropriate read/write permissions
- **Hadoop/AWS SDK**: Ensure correct Hadoop and AWS SDK versions are on Spark classpath
- **GPU Resources**: For GPU jobs, verify RAPIDS and CUDA libraries are properly installed
- **Memory**: Large datasets may require increased executor memory settings

### Debug Commands
```sh
# Check Spark configuration
spark-submit --class com.acceldata.spark.vast.HeavyGpuJobVast --conf spark.sql.adaptive.enabled=false

# Validate S3 connectivity
spark-submit --class com.acceldata.spark.vast.VastS3EnvJob --conf spark.hadoop.fs.s3a.connection.timeout=30000

# Test with smaller datasets
spark-submit --class com.acceldata.spark.vast.SparkIOLoadTest ... write 1000
```

## License

This project is licensed under the Apache License 2.0 - see the full license text below:

```
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

### Third-Party Dependencies

This project uses the following third-party libraries:
- **Apache Spark** (Apache License 2.0)
- **Scala** (Apache License 2.0)
- **Typesafe Config** (Apache License 2.0)
- **RAPIDS for Spark** (Apache License 2.0)

All Scala source files in this project include the Apache License 2.0 header.