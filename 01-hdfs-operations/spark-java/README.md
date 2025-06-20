# Spark Java HDFS Operations Examples

This project demonstrates HDFS read and write operations using Apache Spark with Java. It's part of the Acceldata data platform examples.

## Project Information

- **Group ID**: `io.acceldata`
- **Artifact ID**: `hdfs-operations-examples`
- **Version**: `1.0.0`
- **Company**: Acceldata Inc. (acceldata.io)

## Prerequisites

- Java 11 or higher
- Apache Spark 3.5.0
- Hadoop 3.3.4
- HDFS cluster (or local HDFS setup)

## Project Structure

```
spark-java/
├── build.gradle                 # Gradle build configuration
├── settings.gradle              # Gradle settings
├── gradle/wrapper/              # Gradle wrapper
├── src/main/java/io/acceldata/examples/hdfs/
│   ├── HDFSReadExample.java     # HDFS read operations
│   ├── HDFSWriteExample.java    # HDFS write operations
│   └── HDFSOperationsExample.java # Comprehensive example
└── README.md                    # This file
```

## Examples Included

### 1. HDFSReadExample
Demonstrates various ways to read data from HDFS:
- Reading text files as RDD
- Reading CSV files as DataFrame
- Reading JSON files as DataFrame
- Reading Parquet files as DataFrame
- Reading multiple files with pattern matching

### 2. HDFSWriteExample
Demonstrates various ways to write data to HDFS:
- Writing RDD as text files
- Writing DataFrame as CSV
- Writing DataFrame as JSON
- Writing DataFrame as Parquet
- Writing with partitioning strategies
- Writing with compression

### 3. HDFSOperationsExample
Comprehensive example combining read and write operations in a complete workflow.

## Building the Project

### Using Gradle Wrapper (Recommended)

```bash
# Build the project
./gradlew build

# Create fat JAR for Spark submission
./gradlew fatJar
```

### Using System Gradle

```bash
# Build the project
gradle build

# Create fat JAR for Spark submission
gradle fatJar
```

## Running the Examples

### Containerized Deployment (Kubernetes)

The examples are designed to run as Docker containers in a Kubernetes cluster with the Acceldata data platform.

#### 1. Build Docker Image

```bash
# Build the fat JAR and Docker image
./gradlew buildDockerImage

# Push to registry (configure your registry)
./gradlew pushDockerImage
```

#### 2. Kubernetes Deployment

The data platform will automatically:
- Deploy the container in Kubernetes
- Mount HDFS configuration files:
  - `/etc/krb5.conf`
  - `/etc/hadoop/conf/core-site.xml`
  - `/etc/hadoop/conf/hdfs-site.xml` 
  - `/etc/hadoop/conf/hive-site.xml`
- Mount Kerberos keytab: `/etc/user.keytab`
- Configure HDFS namenode: `hdfs://qenamenode1:8020`

### Local Development and Testing

#### 1. Run HDFS Read Example

```bash
# Using Gradle task (for local development)
./gradlew runHDFSRead

# Using java directly
java -cp build/libs/hdfs-operations-examples-fat-1.6.0.jar \
  io.acceldata.examples.hdfs.HDFSReadExample \
  /data/acceldata-examples
```

#### 2. Run HDFS Write Example

```bash
# Using Gradle task
./gradlew runHDFSWrite

# Using java directly
java -cp build/libs/hdfs-operations-examples-fat-1.0.0.jar \
  io.acceldata.examples.hdfs.HDFSWriteExample \
  /data/acceldata-examples/output
```

#### 3. Run Comprehensive Operations Example

```bash
# Using Gradle task
./gradlew runHDFSOperations

# Using java directly
java -cp build/libs/hdfs-operations-examples-fat-1.0.0.jar \
  io.acceldata.examples.hdfs.HDFSOperationsExample \
  /data/acceldata-examples
```

#### 4. Test Docker Container Locally

```bash
# Run container with mounted configuration files
./gradlew runDockerContainer

# Or manually with docker
docker run --rm \
  -v /etc/krb5.conf:/etc/krb5.conf:ro \
  -v /etc/hadoop/conf:/etc/hadoop/conf:ro \
  -v /etc/user.keytab:/etc/user.keytab:ro \
  acceldata/hdfs-operations-examples:1.6.0
```

## Spark Submit Examples

### Local Mode

```bash
spark-submit \
  --class io.acceldata.examples.hdfs.HDFSOperationsExample \
  --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  build/libs/hdfs-operations-examples-fat-1.6.0.jar \
  hdfs://localhost:9000 \
  /data/acceldata-examples
```

### Cluster Mode

```bash
spark-submit \
  --class io.acceldata.examples.hdfs.HDFSOperationsExample \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 2g \
  --executor-cores 2 \
  --driver-memory 1g \
  --conf spark.hadoop.fs.defaultFS=hdfs://your-namenode:9000 \
  build/libs/hdfs-operations-examples-fat-1.0.0.jar \
  hdfs://your-namenode:9000 \
  /data/acceldata-examples
```

## Configuration

### Containerized Environment Configuration

The examples are configured for containerized deployment with the Acceldata data platform:

#### HDFS Configuration
- **Namenode**: `hdfs://qenamenode1:8020` (configured automatically)
- **Configuration files** (mounted by data platform):
  - `/etc/hadoop/conf/core-site.xml`
  - `/etc/hadoop/conf/hdfs-site.xml`
  - `/etc/hadoop/conf/hive-site.xml`

#### Kerberos Authentication
- **Principal**: `hdfs-adocqecluster@ADSRE.COM`
- **Keytab**: `/etc/user.keytab` (mounted by data platform)
- **Kerberos config**: `/etc/krb5.conf` (mounted by data platform)

### Spark Configuration

The examples include optimized Spark configurations:

```java
SparkConf conf = new SparkConf()
    .setAppName("HDFS Operations Example - Acceldata")
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
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

## Troubleshooting

### Common Issues

1. **HDFS Connection Issues**
   - Verify HDFS namenode is running
   - Check network connectivity
   - Ensure correct HDFS URL format

2. **Permission Issues**
   - Verify HDFS permissions for read/write operations
   - Check user authentication with HDFS

3. **Memory Issues**
   - Adjust Spark executor and driver memory settings
   - Consider data partitioning for large datasets

### Logging

The examples use SLF4J for logging. To see detailed logs, configure your logging level:

```bash
# Set log level to DEBUG
export SPARK_CONF_DIR=/path/to/spark/conf
# Edit log4j.properties to set log level
```

## Contributing

This project is part of the Acceldata data platform examples. For contributions:

1. Follow the existing code structure and naming conventions
2. Add appropriate logging and error handling
3. Include comprehensive documentation
4. Test with different HDFS configurations

## License

Copyright © 2024 Acceldata Inc. All rights reserved.

## Support

For support and questions:
- Visit: https://acceldata.io
- Email: support@acceldata.io 
