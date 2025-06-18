# Azure Blob Storage Operations Examples - Acceldata Data Platform

This repository contains comprehensive examples for Azure Blob Storage operations using Apache Spark across three different platforms: Java, Python, and Jupyter Notebooks. These examples are designed for application developers using the Acceldata data platform.

## Project Information

- **Company**: Acceldata Inc. (acceldata.io)
- **Group ID**: io.acceldata
- **Version**: 1.0.0
- **Purpose**: Educational examples for Azure Blob Storage operations in data platforms

## Repository Structure

```
05-azure-blob/
├── spark-java/                    # Java examples with Gradle
│   ├── src/main/java/io/acceldata/examples/azure/
│   │   ├── AzureBlobReadExample.java
│   │   ├── AzureBlobWriteExample.java
│   │   └── AzureBlobOperationsExample.java
│   ├── build.gradle
│   └── README.md
├── spark-python/                  # Python examples
│   ├── azure_blob_read_example.py
│   ├── azure_blob_write_example.py
│   ├── azure_blob_operations_example.py
│   ├── requirements.txt
│   └── README.md
├── jupyter-notebook/              # Jupyter notebook examples
│   ├── Azure_Blob_Operations_Example.ipynb
│   ├── requirements.txt
│   └── README.md
└── README.md                      # This file
```

## Prerequisites

- **Java 11+** (for Java examples)
- **Python 3.8+** (for Python and Jupyter examples)
- **Apache Spark 3.5.0**
- **Azure Storage Account** (with Blob Storage enabled)
- **Azure SDK for Java** / **Azure SDK for Python**
- **Hadoop Azure 3.3.4** (for ABFS filesystem)
- **Azure credentials** (Storage Account Key, SAS Token, or Service Principal)
- **Docker & Kubernetes** (for containerized Java examples)

## Quick Start

### 1. Java Examples (Containerized Deployment)

```bash
cd spark-java

# Build the project and Docker image
./gradlew build
./gradlew buildDockerImage

# For local development/testing
./gradlew runAzureBlobOperations
./gradlew runAzureBlobRead
./gradlew runAzureBlobWrite

# Deploy to Kubernetes (handled by Acceldata platform)
# The platform will automatically mount:
# - Azure credentials as secrets
# - Storage configuration files
```

### 2. Python Examples

```bash
cd spark-python

# Install dependencies
pip install -r requirements.txt

# Set Azure credentials
export AZURE_STORAGE_ACCOUNT=your_storage_account
export AZURE_STORAGE_KEY=your_storage_key
# OR
export AZURE_CLIENT_ID=your_client_id
export AZURE_CLIENT_SECRET=your_client_secret
export AZURE_TENANT_ID=your_tenant_id

# Run comprehensive example
python azure_blob_operations_example.py

# Run specific examples
python azure_blob_read_example.py
python azure_blob_write_example.py
```

### 3. Jupyter Notebook

```bash
cd jupyter-notebook

# Install dependencies
pip install -r requirements.txt

# Start Jupyter Lab
jupyter lab

# Open Azure_Blob_Operations_Example.ipynb
```

## Examples Overview

### Common Features Across All Platforms

1. **Read Operations**
   - Text files from Azure Blob as RDD
   - CSV files from Azure Blob as DataFrame
   - JSON files from Azure Blob as DataFrame
   - Parquet files from Azure Blob as DataFrame
   - Multiple files with prefix/pattern matching
   - Partitioned data reading

2. **Write Operations**
   - RDD as text files to Azure Blob
   - DataFrame as CSV to Azure Blob
   - DataFrame as JSON to Azure Blob
   - DataFrame as Parquet to Azure Blob
   - Partitioned data writing
   - Compressed data writing

3. **Advanced Operations**
   - Container operations (list, create, delete)
   - Blob metadata handling
   - Large file uploads with chunking
   - Server-side encryption
   - Access tier management (Hot, Cool, Archive)
   - Lifecycle policy management

### Platform-Specific Features

#### Java (Gradle + Docker)
- Enterprise-grade Azure SDK integration
- Containerized deployment for Kubernetes
- Service principal-based authentication
- Azure Blob Transfer Manager for large files
- Comprehensive error handling and logging
- Fat JAR creation for Spark submission
- Docker image building and deployment

#### Python
- Object-oriented design with classes
- Type hints for better code documentation
- Azure Blob Storage client library integration
- Comprehensive logging
- Modular structure for reusability

#### Jupyter Notebook
- Interactive data exploration
- Step-by-step explanations
- Data visualization capabilities
- Educational format with markdown documentation

## Configuration

### Azure Blob Storage Setup

Ensure your Azure Storage Account and container are created:

```bash
# Create storage account (using Azure CLI)
az storage account create \
    --name acceldata-examples \
    --resource-group myResourceGroup \
    --location eastus \
    --sku Standard_LRS

# Create container
az storage container create \
    --name data \
    --account-name acceldata-examples

# Create sample directories (virtual)
az storage blob upload \
    --file /dev/null \
    --name input/.keep \
    --container-name data \
    --account-name acceldata-examples
```

### Spark Configuration

All examples include optimized Spark configurations for Azure Blob:

```
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.hadoop.fs.azure.account.key.acceldata-examples.blob.core.windows.net=YOUR_STORAGE_KEY
spark.hadoop.fs.azure.account.auth.type.acceldata-examples.blob.core.windows.net=SharedKey
```

### ABFS (Azure Data Lake Storage Gen2) Configuration

```
spark.hadoop.fs.azure.account.key.acceldata-examples.dfs.core.windows.net=YOUR_STORAGE_KEY
spark.hadoop.fs.azure.account.auth.type.acceldata-examples.dfs.core.windows.net=SharedKey
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

### 3. Azure Blob Optimization
- Use appropriate access tiers (Hot, Cool, Archive)
- Enable block blob uploads for large files
- Configure appropriate block size
- Use Azure CDN for frequently accessed data

### 4. Compression
- **Snappy**: Fast compression/decompression
- **GZIP**: Higher compression ratio
- **LZ4**: Good balance for streaming

## Spark Submit Examples

### Local Mode
```bash
spark-submit \
  --class io.acceldata.examples.azure.AzureBlobOperationsExample \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6 \
  --conf spark.hadoop.fs.azure.account.key.acceldata-examples.blob.core.windows.net=YOUR_STORAGE_KEY \
  spark-java/build/libs/azure-blob-operations-examples-fat-1.0.0.jar
```

### Cluster Mode
```bash
spark-submit \
  --class io.acceldata.examples.azure.AzureBlobOperationsExample \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 2g \
  --executor-cores 2 \
  --packages org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6 \
  spark-java/build/libs/azure-blob-operations-examples-fat-1.0.0.jar
```

## Azure Blob Path Examples

### Standard Blob Storage Paths
```
wasbs://data@acceldata-examples.blob.core.windows.net/input/
wasbs://data@acceldata-examples.blob.core.windows.net/output/parquet/
wasbs://data@acceldata-examples.blob.core.windows.net/logs/2024/01/01/
```

### ABFS (Data Lake Gen2) Paths
```
abfss://data@acceldata-examples.dfs.core.windows.net/input/
abfss://data@acceldata-examples.dfs.core.windows.net/output/parquet/
abfss://data@acceldata-examples.dfs.core.windows.net/logs/2024/01/01/
```

### Partitioned Paths
```
abfss://data@acceldata-examples.dfs.core.windows.net/partitioned/year=2024/month=01/day=01/
abfss://data@acceldata-examples.dfs.core.windows.net/partitioned/region=eastus/department=engineering/
```

## Security Best Practices

### 1. Authentication Methods
- **Storage Account Key**: Simple but less secure
- **SAS Token**: Time-limited access with specific permissions
- **Service Principal**: Recommended for production environments
- **Managed Identity**: Best for Azure-hosted applications

### 2. Encryption
- Enable storage service encryption (SSE)
- Use customer-managed keys for enhanced security
- Implement encryption in transit

### 3. Access Control
- Use Azure RBAC for fine-grained access control
- Implement least privilege access
- Enable Azure Storage Analytics logging

## Troubleshooting

### Common Issues

1. **Azure Blob Access Denied**
   ```bash
   # Check storage account permissions
   az storage account show --name acceldata-examples --resource-group myResourceGroup
   
   # Verify container permissions
   az storage container show-permission --name data --account-name acceldata-examples
   ```

2. **Slow Azure Blob Performance**
   ```bash
   # Use premium storage for better performance
   az storage account create --sku Premium_LRS --kind BlockBlobStorage
   ```

3. **Large File Upload Issues**
   ```bash
   # Increase block size for large files
   --conf spark.hadoop.fs.azure.block.size=134217728
   ```

## Azure CLI Integration

### Useful Commands

```bash
# List container contents
az storage blob list --container-name data --account-name acceldata-examples --output table

# Upload files to Azure Blob
az storage blob upload --file local-file.csv --name input/sample.csv --container-name data --account-name acceldata-examples

# Download files from Azure Blob
az storage blob download --name input/sample.csv --file downloaded-file.csv --container-name data --account-name acceldata-examples

# Set blob metadata
az storage blob metadata update --name input/sample.csv --container-name data --account-name acceldata-examples --metadata project=acceldata environment=dev
```

## Integration with Azure Services

### Azure Synapse Analytics Integration
```python
# Read from Synapse
df = spark.read \
    .format("com.databricks.spark.sqldw") \
    .option("url", "jdbc:sqlserver://server.database.windows.net:1433;database=database") \
    .option("tempDir", "abfss://temp@storage.dfs.core.windows.net/") \
    .option("forwardSparkAzureStorageCredentials", "true") \
    .option("dbTable", "schema.table") \
    .load()
```

### Azure Data Factory Integration
```json
{
    "name": "SparkActivity",
    "type": "HDInsightSpark",
    "linkedServiceName": {
        "referenceName": "HDInsightLinkedService",
        "type": "LinkedServiceReference"
    },
    "typeProperties": {
        "rootPath": "adl://storage.azuredatalakestore.net/spark/",
        "entryFilePath": "azure-blob-operations-examples-fat-1.0.0.jar",
        "className": "io.acceldata.examples.azure.AzureBlobOperationsExample"
    }
}
```

## Development Guidelines

### Code Standards
- Follow language-specific conventions
- Include comprehensive error handling
- Add logging for debugging and monitoring
- Document all public methods and classes

### Testing
- Test with different Azure regions
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

1. **Data Engineers**: Learning Azure Blob integration patterns
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
- [Apache Spark on Azure Guide](https://docs.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-overview)
- [Azure Blob Storage Documentation](https://docs.microsoft.com/en-us/azure/storage/blobs/)
- [Hadoop Azure Documentation](https://hadoop.apache.org/docs/current/hadoop-azure/index.html)

## Roadmap

Future enhancements planned:
- [ ] Azure Synapse Analytics integration
- [ ] Azure Data Factory integration
- [ ] Streaming data examples
- [ ] Advanced security configurations
- [ ] Performance benchmarking tools
- [ ] Multi-region deployment examples

---

**Built with ❤️ by the Acceldata Platform Team** 