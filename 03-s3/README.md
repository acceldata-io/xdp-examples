# S3 Operations Examples - Acceldata Data Platform

This repository contains comprehensive examples for Amazon S3 operations using Apache Spark across three different platforms: Java, Python, and Jupyter Notebooks. These examples are designed for application developers using the Acceldata data platform.

## Project Information

- **Company**: Acceldata Inc. (acceldata.io)
- **Group ID**: io.acceldata
- **Version**: 1.0.0
- **Purpose**: Educational examples for S3 operations in data platforms

## Repository Structure

```
03-s3/
├── spark-java/                    # Java examples with Gradle
│   ├── src/main/java/io/acceldata/examples/s3/
│   │   ├── S3ReadExample.java
│   │   ├── S3WriteExample.java
│   │   └── S3OperationsExample.java
│   ├── build.gradle
│   └── README.md
├── spark-python/                  # Python examples
│   ├── s3_read_example.py
│   ├── s3_write_example.py
│   ├── s3_operations_example.py
│   ├── requirements.txt
│   └── README.md
├── jupyter-notebook/              # Jupyter notebook examples
│   ├── S3_Operations_Example.ipynb
│   ├── requirements.txt
│   └── README.md
└── README.md                      # This file
```

## Prerequisites

- **Java 11+** (for Java examples)
- **Python 3.8+** (for Python and Jupyter examples)
- **Apache Spark 3.5.0**
- **AWS S3 bucket** (accessible with proper credentials)
- **AWS SDK for Java 2.20.0** / **Boto3 for Python**
- **Hadoop AWS 3.3.4** (for S3A filesystem)
- **AWS credentials** (Access Key/Secret Key or IAM roles)
- **Docker & Kubernetes** (for containerized Java examples)

## Quick Start

### 1. Java Examples (Containerized Deployment)

```bash
cd spark-java

# Build the project and Docker image
./gradlew build
./gradlew buildDockerImage

# For local development/testing
./gradlew runS3Operations
./gradlew runS3Read
./gradlew runS3Write

# Deploy to Kubernetes (handled by Acceldata platform)
# The platform will automatically mount:
# - AWS credentials as secrets
# - S3 configuration files
```

### 2. Python Examples

```bash
cd spark-python

# Install dependencies
pip install -r requirements.txt

# Set AWS credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1

# Run comprehensive example
python s3_operations_example.py

# Run specific examples
python s3_read_example.py
python s3_write_example.py
```

### 3. Jupyter Notebook

```bash
cd jupyter-notebook

# Install dependencies
pip install -r requirements.txt

# Start Jupyter Lab
jupyter lab

# Open S3_Operations_Example.ipynb
```

## Examples Overview

### Common Features Across All Platforms

1. **Read Operations**
   - Text files from S3 as RDD
   - CSV files from S3 as DataFrame
   - JSON files from S3 as DataFrame
   - Parquet files from S3 as DataFrame
   - Multiple files with prefix/pattern matching
   - Partitioned data reading

2. **Write Operations**
   - RDD as text files to S3
   - DataFrame as CSV to S3
   - DataFrame as JSON to S3
   - DataFrame as Parquet to S3
   - Partitioned data writing
   - Compressed data writing

3. **Advanced Operations**
   - S3 bucket operations (list, create, delete)
   - Object metadata handling
   - Multi-part uploads for large files
   - Server-side encryption (SSE-S3, SSE-KMS)
   - Cross-region replication setup
   - Lifecycle policy management

### Platform-Specific Features

#### Java (Gradle + Docker)
- Enterprise-grade AWS SDK integration
- Containerized deployment for Kubernetes
- IAM role-based authentication
- S3 Transfer Manager for large files
- Comprehensive error handling and logging
- Fat JAR creation for Spark submission
- Docker image building and deployment

#### Python
- Object-oriented design with classes
- Type hints for better code documentation
- Boto3 integration for advanced S3 operations
- Comprehensive logging
- Modular structure for reusability

#### Jupyter Notebook
- Interactive data exploration
- Step-by-step explanations
- Data visualization capabilities
- Educational format with markdown documentation

## Configuration

### AWS S3 Setup

Ensure your S3 bucket is created and accessible:

```bash
# Create S3 bucket (using AWS CLI)
aws s3 mb s3://acceldata-examples-bucket --region us-east-1

# Set bucket policy for Spark access
aws s3api put-bucket-policy --bucket acceldata-examples-bucket --policy file://bucket-policy.json

# Create sample directories
aws s3api put-object --bucket acceldata-examples-bucket --key data/input/
aws s3api put-object --bucket acceldata-examples-bucket --key data/output/
```

### Spark Configuration

All examples include optimized Spark configurations for S3:

```
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.fast.upload=true
spark.hadoop.fs.s3a.block.size=128M
spark.hadoop.fs.s3a.multipart.size=64M
```

## Data Formats Supported

| Format | Read | Write | Compression | Partitioning | Best Use Case |
|--------|------|-------|-------------|--------------|---------------|
| Parquet | ✅ | ✅ | ✅ | ✅ | Analytics, Data Lake |
| CSV | ✅ | ✅ | ✅ | ❌ | Data Exchange |
| JSON | ✅ | ✅ | ✅ | ❌ | Semi-structured Data |
| Text | ✅ | ✅ | ✅ | ❌ | Log Files |
| Avro | ✅ | ✅ | ✅ | ✅ | Schema Evolution |
| ORC | ✅ | ✅ | ✅ | ✅ | Hive Integration |

## Performance Best Practices

### 1. File Format Selection
- **Parquet**: Best for analytics workloads
- **Avro**: Good for schema evolution
- **ORC**: Optimized for Hive integration

### 2. Partitioning Strategy
- Partition by frequently filtered columns (date, region, etc.)
- Avoid too many small partitions
- Aim for 128MB-1GB per partition

### 3. S3 Optimization
- Use S3A filesystem (not S3N)
- Enable fast upload for large files
- Configure appropriate multipart upload size
- Use S3 Transfer Acceleration for cross-region

### 4. Compression
- **Snappy**: Fast compression/decompression
- **GZIP**: Higher compression ratio
- **LZ4**: Good balance for streaming

## Spark Submit Examples

### Local Mode
```bash
spark-submit \
  --class io.acceldata.examples.s3.S3OperationsExample \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET_KEY \
  spark-java/build/libs/s3-operations-examples-fat-1.0.0.jar
```

### Cluster Mode
```bash
spark-submit \
  --class io.acceldata.examples.s3.S3OperationsExample \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 2g \
  --executor-cores 2 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  spark-java/build/libs/s3-operations-examples-fat-1.0.0.jar
```

## S3 Path Examples

### Standard S3 Paths
```
s3a://acceldata-examples-bucket/data/input/
s3a://acceldata-examples-bucket/data/output/parquet/
s3a://acceldata-examples-bucket/logs/2024/01/01/
```

### Partitioned Paths
```
s3a://acceldata-examples-bucket/data/partitioned/year=2024/month=01/day=01/
s3a://acceldata-examples-bucket/data/partitioned/region=us-east-1/department=engineering/
```

## Security Best Practices

### 1. Credential Management
- Use IAM roles instead of access keys when possible
- Implement credential rotation
- Use AWS Secrets Manager for sensitive data

### 2. Encryption
- Enable server-side encryption (SSE-S3 or SSE-KMS)
- Use client-side encryption for highly sensitive data
- Implement encryption in transit

### 3. Access Control
- Use S3 bucket policies and IAM policies
- Implement least privilege access
- Enable S3 access logging

## Troubleshooting

### Common Issues

1. **S3 Access Denied**
   ```bash
   # Check IAM permissions
   aws iam get-user-policy --user-name spark-user --policy-name S3Access
   
   # Verify bucket policy
   aws s3api get-bucket-policy --bucket acceldata-examples-bucket
   ```

2. **Slow S3 Performance**
   ```bash
   # Enable S3 Transfer Acceleration
   aws s3api put-bucket-accelerate-configuration --bucket acceldata-examples-bucket --accelerate-configuration Status=Enabled
   ```

3. **Large File Upload Issues**
   ```bash
   # Increase multipart upload size
   --conf spark.hadoop.fs.s3a.multipart.size=128M
   --conf spark.hadoop.fs.s3a.multipart.threshold=256M
   ```

## AWS CLI Integration

### Useful Commands

```bash
# List bucket contents
aws s3 ls s3://acceldata-examples-bucket/data/ --recursive

# Copy files to S3
aws s3 cp local-file.csv s3://acceldata-examples-bucket/data/input/

# Sync directories
aws s3 sync ./local-data/ s3://acceldata-examples-bucket/data/input/

# Set object metadata
aws s3api put-object --bucket acceldata-examples-bucket --key data/sample.csv --metadata project=acceldata,environment=dev
```

## Development Guidelines

### Code Standards
- Follow language-specific conventions
- Include comprehensive error handling
- Add logging for debugging and monitoring
- Document all public methods and classes

### Testing
- Test with different S3 regions
- Validate data integrity after operations
- Test with various file sizes
- Include edge case handling

### Documentation
- Keep README files updated
- Include code comments for complex logic
- Provide usage examples
- Document configuration requirements

## Use Cases

These examples are suitable for:

1. **Data Engineers**: Learning S3 integration patterns
2. **Application Developers**: Understanding cloud storage connectivity
3. **Data Scientists**: Accessing data lake for analysis
4. **DevOps Teams**: Deploying data processing applications
5. **Students**: Learning cloud storage with Spark

## Integration with Acceldata Platform

These examples demonstrate patterns commonly used in:

- **Data Lake Operations**
- **ETL/ELT Processes**
- **Backup and Archival**
- **Cross-region Data Replication**
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
- [Apache Spark on S3 Guide](https://spark.apache.org/docs/latest/cloud-integration.html)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Hadoop S3A Documentation](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)

## Roadmap

Future enhancements planned:
- [ ] S3 Select integration
- [ ] AWS Glue Catalog integration
- [ ] Streaming data examples
- [ ] Advanced security configurations
- [ ] Performance benchmarking tools
- [ ] Multi-region deployment examples

---

**Built with ❤️ by the Acceldata Platform Team** 