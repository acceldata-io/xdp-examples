# Python Spark Examples

## Docker Build Commands

```bash
# Build the Docker image
docker build --platform linux/amd64 -t spark-python-examples:latest .

# Build with specific tag
docker build --platform linux/amd64 -t spark-python-examples:v1.0.0 .

# Build with custom base image
docker build --platform linux/amd64 --build-arg BASE_IMAGE=public.ecr.aws/l7l2s8m9/spark-python-jdk-ubuntu:3.5.1-2.12-3.11.13-11.0.27_6-24.04-20250529-20250604 -t nitinupadhyayaacceldata/xdpexamples:Python-Example .

# Build with platform specification
docker build --platform linux/amd64 -t nitinupadhyayaacceldata/xdpexamples:Python-Example .
```

## Environment Variables

### HDFS Operations (ODP)
- `URL`: HDFS cluster URL (e.g., `hdfs://namenode:8020`)
- `KERBEROS_PRINCIPAL`: Kerberos principal for authentication
- `KERBEROS_KEYTAB`: Path to Kerberos keytab file
- `HDFS_FILE_PATH`: Input file path in HDFS
- `HDFS_FILE_OUTPUT_PATH`: Output file path in HDFS (for write operations)

### S3 Operations
- `DATASTORE_AWS_ACCESS_KEY_ID`: AWS access key ID
- `DATASTORE_AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `DATASTORE_S3_BUCKET_NAME`: S3 bucket name
- `DATASTORE_S3_FILE_PATH`: Input file path in S3
- `DATASTORE_S3_FILE_PATH_OUTPUT`: Output file path in S3 (for write operations)
- `DATASTORE_S3_REGION`: AWS region