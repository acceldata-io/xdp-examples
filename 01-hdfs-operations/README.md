# HDFS Operations Examples - Acceldata Data Platform

This repository contains comprehensive examples for HDFS read and write operations using Apache Spark across three different platforms: Java, Python, and Jupyter Notebooks. These examples are designed for application developers using the Acceldata data platform.

## Project Information

- **Company**: Acceldata Inc. (acceldata.io)
- **Group ID**: io.acceldata
- **Version**: 1.0.0
- **Purpose**: Educational examples for HDFS operations in data platforms

## Repository Structure

```
01-hdfs-operations/
├── spark-java/                    # Java examples with Gradle
│   ├── src/main/java/io/acceldata/examples/hdfs/
│   │   ├── HDFSReadExample.java
│   │   ├── HDFSWriteExample.java
│   │   └── HDFSOperationsExample.java
│   ├── build.gradle
│   └── README.md
├── spark-python/                  # Python examples
│   ├── hdfs_read_example.py
│   ├── hdfs_write_example.py
│   ├── hdfs_operations_example.py
│   ├── requirements.txt
│   └── README.md
├── jupyter-notebook/              # Jupyter notebook examples
│   ├── HDFS_Operations_Example.ipynb
│   ├── requirements.txt
│   └── README.md
└── README.md                      # This file
```

## Prerequisites

- **Java 11+** (for Java examples)
- **Python 3.8+** (for Python and Jupyter examples)
- **Apache Spark 3.5.0**
- **Hadoop 3.3.4**
- **HDFS cluster** (accessible at hdfs://qenamenode1:8020)
- **Kerberos authentication** (for containerized deployment)
- **Docker & Kubernetes** (for containerized Java examples)

## Quick Start

### 1. Java Examples (Containerized Deployment)

```bash
cd spark-java

# Build the project and Docker image
./gradlew build
./gradlew buildDockerImage

# For local development/testing
./gradlew runHDFSOperations
./gradlew runHDFSRead
./gradlew runHDFSWrite

# Deploy to Kubernetes (handled by Acceldata platform)
# The platform will automatically mount:
# - /etc/krb5.conf
# - /etc/hadoop/conf/*.xml
# - /etc/user.keytab
```

### 2. Python Examples

```bash
cd spark-python

# Install dependencies
pip install -r requirements.txt

# Run comprehensive example
python hdfs_operations_example.py

# Run specific examples
python hdfs_read_example.py
python hdfs_write_example.py
```

### 3. Jupyter Notebook

```bash
cd jupyter-notebook

# Install dependencies
pip install -r requirements.txt

# Start Jupyter Lab
jupyter lab

# Open HDFS_Operations_Example.ipynb
```

## Examples Overview

### Common Features Across All Platforms

1. **Read Operations**
   - Text files as RDD
   - CSV files as DataFrame
   - JSON files as DataFrame
   - Parquet files as DataFrame
   - Multiple files with pattern matching

2. **Write Operations**
   - RDD as text files
   - DataFrame as CSV
   - DataFrame as JSON
   - DataFrame as Parquet
   - Partitioned data
   - Compressed data

3. **Advanced Operations**
   - Data analysis and transformations
   - JOIN operations
   - Aggregations and reporting
   - Performance optimizations

### Platform-Specific Features

#### Java (Gradle + Docker)
- Enterprise-grade structure with proper packaging
- Containerized deployment for Kubernetes
- Kerberos authentication with keytab
- Automatic HDFS configuration loading
- Comprehensive error handling and logging
- Fat JAR creation for Spark submission
- Docker image building and deployment

#### Python
- Object-oriented design with classes
- Type hints for better code documentation
- Comprehensive logging
- Modular structure for reusability

#### Jupyter Notebook
- Interactive data exploration
- Step-by-step explanations
- Visualization capabilities
- Educational format with markdown documentation

## Configuration

### HDFS Setup

Ensure your HDFS cluster is running:

```bash
# Start HDFS (if running locally)
start-dfs.sh

# Check HDFS status
hdfs dfsadmin -report

# Create base directories
hdfs dfs -mkdir -p /data/acceldata-examples
```

### Spark Configuration

All examples include optimized Spark configurations:

```
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

## Data Formats Supported

| Format | Read | Write | Compression | Partitioning | Best Use Case |
|--------|------|-------|-------------|--------------|---------------|
| Parquet | ✅ | ✅ | ✅ | ✅ | Analytics, Data Warehousing |
| CSV | ✅ | ✅ | ❌ | ❌ | Data Exchange, Human Readable |
| JSON | ✅ | ✅ | ❌ | ❌ | Semi-structured Data |
| Text | ✅ | ✅ | ❌ | ❌ | Log Files, Simple Data |

## Performance Best Practices

### 1. File Format Selection
- **Parquet**: Best for analytics workloads
- **CSV**: Good for data exchange
- **JSON**: Flexible schema support

### 2. Partitioning Strategy
- Partition by frequently filtered columns
- Avoid too many small partitions
- Aim for 128MB-1GB per partition

### 3. Compression
- **Snappy**: Fast compression/decompression
- **GZIP**: Higher compression ratio
- **LZ4**: Good balance

### 4. Caching
- Cache frequently accessed DataFrames
- Use appropriate storage levels
- Unpersist when no longer needed

## Spark Submit Examples

### Local Mode
```bash
spark-submit \
  --class io.acceldata.examples.hdfs.HDFSOperationsExample \
  --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  spark-java/build/libs/hdfs-operations-examples-fat-1.0.0.jar
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
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  spark-java/build/libs/hdfs-operations-examples-fat-1.0.0.jar
```

## Troubleshooting

### Common Issues

1. **HDFS Connection**
   ```bash
   # Check HDFS configuration
   hdfs getconf -confKey fs.defaultFS
   
   # Test connectivity
   hdfs dfs -ls /
   ```

2. **Spark Memory**
   ```bash
   # Increase memory settings
   export SPARK_DRIVER_MEMORY=2g
   export SPARK_EXECUTOR_MEMORY=2g
   ```

3. **Permissions**
   ```bash
   # Check HDFS permissions
   hdfs dfs -ls -la /data
   
   # Fix permissions if needed
   hdfs dfs -chmod 755 /data/acceldata-examples
   ```

## Development Guidelines

### Code Standards
- Follow language-specific conventions (Java: camelCase, Python: snake_case)
- Include comprehensive error handling
- Add logging for debugging and monitoring
- Document all public methods and classes

### Testing
- Test with different HDFS configurations
- Validate data integrity after write operations
- Test with various data sizes
- Include edge case handling

### Documentation
- Keep README files updated
- Include code comments for complex logic
- Provide usage examples
- Document configuration requirements

## Use Cases

These examples are suitable for:

1. **Data Engineers**: Learning HDFS integration patterns
2. **Application Developers**: Understanding data platform APIs
3. **Data Scientists**: Accessing data for analysis
4. **DevOps Teams**: Deploying data processing applications
5. **Students**: Learning big data technologies

## Integration with Acceldata Platform

These examples demonstrate patterns commonly used in:

- **Data Ingestion Pipelines**
- **ETL/ELT Processes**
- **Data Lake Operations**
- **Analytics Workloads**
- **Data Quality Checks**
- **Monitoring and Observability**

## Contributing

We welcome contributions to improve these examples:

1. Fork the repository
2. Create a feature branch
3. Add your improvements
4. Test thoroughly
5. Submit a pull request

### Contribution Guidelines
- Follow existing code structure
- Add appropriate tests
- Update documentation
- Ensure compatibility across platforms

## License

Copyright © 2024 Acceldata Inc. All rights reserved.

These examples are provided for educational purposes as part of the Acceldata data platform documentation.

## Support and Resources

### Support
- **Website**: https://acceldata.io
- **Email**: support@acceldata.io
- **Documentation**: [Acceldata Platform Docs]

### Additional Resources
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Hadoop HDFS Guide](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
- [Acceldata Platform Overview](https://acceldata.io/platform)

## Roadmap

Future enhancements planned:
- [ ] Integration with cloud storage (S3, Azure Blob, GCS)
- [ ] Streaming data examples
- [ ] Machine learning pipeline examples
- [ ] Advanced security configurations
- [ ] Performance benchmarking tools
- [ ] Docker containerization examples

---

**Built with ❤️ by the Acceldata Platform Team** 