# Google Cloud Storage (GCS) Operations Examples - Acceldata Data Platform

This repository contains comprehensive examples for Google Cloud Storage operations using Apache Spark across three different platforms: Java, Python, and Jupyter Notebooks. These examples are designed for application developers using the Acceldata data platform.

## Project Information

- **Company**: Acceldata Inc. (acceldata.io)
- **Group ID**: io.acceldata
- **Version**: 1.0.0
- **Purpose**: Educational examples for GCS operations in data platforms

## Repository Structure

```
04-gcs/
├── spark-java/                    # Java examples with Gradle
│   ├── src/main/java/io/acceldata/examples/gcs/
│   │   ├── GCSReadExample.java
│   │   ├── GCSWriteExample.java
│   │   └── GCSOperationsExample.java
│   ├── build.gradle
│   └── README.md
├── spark-python/                  # Python examples
│   ├── gcs_read_example.py
│   ├── gcs_write_example.py
│   ├── gcs_operations_example.py
│   ├── requirements.txt
│   └── README.md
├── jupyter-notebook/              # Jupyter notebook examples
│   ├── GCS_Operations_Example.ipynb
│   ├── requirements.txt
│   └── README.md
└── README.md                      # This file
```

## Prerequisites

- **Java 11+** (for Java examples)
- **Python 3.8+** (for Python and Jupyter examples)
- **Apache Spark 3.5.0**
- **Google Cloud Storage bucket** (accessible with proper credentials)
- **Google Cloud SDK** / **Google Cloud Client Libraries**
- **Hadoop GCS Connector 2.2.11**
- **GCP Service Account** (with Storage permissions)
- **Docker & Kubernetes** (for containerized Java examples)

## Quick Start

### 1. Java Examples (Containerized Deployment)

```bash
cd spark-java

# Build the project and Docker image
./gradlew build
./gradlew buildDockerImage

# For local development/testing
./gradlew runGCSOperations
./gradlew runGCSRead
./gradlew runGCSWrite

# Deploy to Kubernetes (handled by Acceldata platform)
# The platform will automatically mount:
# - GCP service account key as secrets
# - GCS configuration files
```

### 2. Python Examples

```bash
cd spark-python

# Install dependencies
pip install -r requirements.txt

# Set GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
export GCP_PROJECT_ID=your-project-id

# Run comprehensive example
python gcs_operations_example.py

# Run specific examples
python gcs_read_example.py
python gcs_write_example.py
```

### 3. Jupyter Notebook

```bash
cd jupyter-notebook

# Install dependencies
pip install -r requirements.txt

# Start Jupyter Lab
jupyter lab

# Open GCS_Operations_Example.ipynb
```

## Examples Overview

### Common Features Across All Platforms

1. **Read Operations**
   - Text files from GCS as RDD
   - CSV files from GCS as DataFrame
   - JSON files from GCS as DataFrame
   - Parquet files from GCS as DataFrame
   - Multiple files with prefix/pattern matching
   - Partitioned data reading

2. **Write Operations**
   - RDD as text files to GCS
   - DataFrame as CSV to GCS
   - DataFrame as JSON to GCS
   - DataFrame as Parquet to GCS
   - Partitioned data writing
   - Compressed data writing

3. **Advanced Operations**
   - GCS bucket operations (list, create, delete)
   - Object metadata handling
   - Resumable uploads for large files
   - Customer-managed encryption keys (CMEK)
   - Cross-region replication setup
   - Lifecycle policy management

### Platform-Specific Features

#### Java (Gradle + Docker)
- Enterprise-grade Google Cloud SDK integration
- Containerized deployment for Kubernetes
- Service account-based authentication
- GCS Transfer Manager for large files
- Comprehensive error handling and logging
- Fat JAR creation for Spark submission
- Docker image building and deployment

#### Python
- Object-oriented design with classes
- Type hints for better code documentation
- Google Cloud Storage client library integration
- Comprehensive logging
- Modular structure for reusability

#### Jupyter Notebook
- Interactive data exploration
- Step-by-step explanations
- Data visualization capabilities
- Educational format with markdown documentation

## Configuration

### Google Cloud Storage Setup

Ensure your GCS bucket is created and accessible:

```bash
# Create GCS bucket (using gcloud CLI)
gsutil mb gs://acceldata-examples-bucket

# Set bucket permissions
gsutil iam ch serviceAccount:spark-service-account@your-project.iam.gserviceaccount.com:objectAdmin gs://acceldata-examples-bucket

# Create sample directories
gsutil cp /dev/null gs://acceldata-examples-bucket/data/input/.keep
gsutil cp /dev/null gs://acceldata-examples-bucket/data/output/.keep
```

### Spark Configuration

All examples include optimized Spark configurations for GCS:

```
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS
spark.hadoop.google.cloud.auth.service.account.enable=true
spark.hadoop.google.cloud.auth.service.account.json.keyfile=/path/to/keyfile.json
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

### 3. GCS Optimization
- Use appropriate storage classes (Standard, Nearline, Coldline)
- Enable parallel composite uploads for large files
- Configure appropriate chunk size
- Use regional buckets for better performance

### 4. Compression
- **Snappy**: Fast compression/decompression
- **GZIP**: Higher compression ratio
- **LZ4**: Good balance for streaming

## Spark Submit Examples

### Local Mode
```bash
spark-submit \
  --class io.acceldata.examples.gcs.GCSOperationsExample \
  --master local[*] \
  --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11 \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/path/to/keyfile.json \
  spark-java/build/libs/gcs-operations-examples-fat-1.0.0.jar
```

### Cluster Mode
```bash
spark-submit \
  --class io.acceldata.examples.gcs.GCSOperationsExample \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 2g \
  --executor-cores 2 \
  --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11 \
  spark-java/build/libs/gcs-operations-examples-fat-1.0.0.jar
```

## GCS Path Examples

### Standard GCS Paths
```
gs://acceldata-examples-bucket/data/input/
gs://acceldata-examples-bucket/data/output/parquet/
gs://acceldata-examples-bucket/logs/2024/01/01/
```

### Partitioned Paths
```
gs://acceldata-examples-bucket/data/partitioned/year=2024/month=01/day=01/
gs://acceldata-examples-bucket/data/partitioned/region=us-central1/department=engineering/
```

## Security Best Practices

### 1. Authentication
- Use service accounts instead of user accounts
- Implement service account key rotation
- Use Workload Identity for GKE deployments

### 2. Encryption
- Enable customer-managed encryption keys (CMEK)
- Use client-side encryption for highly sensitive data
- Implement encryption in transit

### 3. Access Control
- Use IAM policies for fine-grained access control
- Implement least privilege access
- Enable Cloud Audit Logs

## Troubleshooting

### Common Issues

1. **GCS Access Denied**
   ```bash
   # Check service account permissions
   gcloud projects get-iam-policy your-project-id
   
   # Verify bucket permissions
   gsutil iam get gs://acceldata-examples-bucket
   ```

2. **Slow GCS Performance**
   ```bash
   # Use regional buckets
   gsutil mb -l us-central1 gs://acceldata-examples-bucket-regional
   ```

3. **Large File Upload Issues**
   ```bash
   # Enable parallel composite uploads
   gsutil -o GSUtil:parallel_composite_upload_threshold=150M cp large-file.csv gs://bucket/
   ```

## Google Cloud CLI Integration

### Useful Commands

```bash
# List bucket contents
gsutil ls gs://acceldata-examples-bucket/data/ -r

# Copy files to GCS
gsutil cp local-file.csv gs://acceldata-examples-bucket/data/input/

# Sync directories
gsutil -m rsync -r ./local-data/ gs://acceldata-examples-bucket/data/input/

# Set object metadata
gsutil setmeta -h "x-goog-meta-project:acceldata" -h "x-goog-meta-environment:dev" gs://acceldata-examples-bucket/data/sample.csv
```

## Integration with Google Cloud Services

### BigQuery Integration
```python
# Read from BigQuery
df = spark.read.format("bigquery") \
    .option("table", "project.dataset.table") \
    .load()

# Write to BigQuery
df.write.format("bigquery") \
    .option("table", "project.dataset.table") \
    .mode("overwrite") \
    .save()
```

### Dataproc Integration
```bash
# Submit Spark job to Dataproc
gcloud dataproc jobs submit spark \
    --cluster=my-cluster \
    --region=us-central1 \
    --class=io.acceldata.examples.gcs.GCSOperationsExample \
    --jars=gs://acceldata-examples-bucket/jars/gcs-operations-examples-fat-1.0.0.jar
```

## Development Guidelines

### Code Standards
- Follow language-specific conventions
- Include comprehensive error handling
- Add logging for debugging and monitoring
- Document all public methods and classes

### Testing
- Test with different GCS regions
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

1. **Data Engineers**: Learning GCS integration patterns
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
- [Apache Spark on GCP Guide](https://cloud.google.com/dataproc/docs/concepts/spark-overview)
- [Google Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [GCS Connector Documentation](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage)

## Roadmap

Future enhancements planned:
- [ ] BigQuery integration examples
- [ ] Dataproc serverless examples
- [ ] Streaming data examples
- [ ] Advanced security configurations
- [ ] Performance benchmarking tools
- [ ] Multi-region deployment examples

---

**Built with ❤️ by the Acceldata Platform Team** 