# VastS3EnvJob

## Overview

`VastS3EnvJob` is a Scala Spark application that demonstrates reading AWS S3 configuration from environment variables and performing basic read/write operations on a VAST S3-compatible object store. The job is designed to validate that the Spark environment can access S3 storage using credentials and configuration supplied via environment variables, and also checks for GPU resource availability in the Spark cluster.

## Purpose

This project is intended to:

- Serve as a reference Spark job for validating S3 access in environments where credentials are supplied via environment variables.
- Validate GPU resource detection in Spark.
- Provide a minimal, reproducible job for testing S3 integration on VAST or S3-compatible storage systems.

## The `VastS3EnvJob` Class

The main logic is implemented in the `VastS3EnvJob` Scala object. Its key features include:

- **Reads S3 credentials and connection details from environment variables.**
- **Configures Spark to use the S3A connector with the supplied credentials.**
- **Creates and writes a sample DataFrame to S3, then reads it back and displays the data.**
- **Checks for GPU resources in the Spark runtime configuration and prints a message about GPU/CPU usage.**

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

## Build Steps

1. **Clone the repository:**
   ```sh
   git clone <repo-url>
   cd vasts3-test-job
   ```

2. **Build the fat JAR:**
    - Using `sbt`:
      ```sh
      sbt assembly
      ```
    - Or using `mvn`:
      ```sh
      mvn package
      ```
   This will produce a JAR in the `target/scala-2.12/` directory (path may vary).

## Running the Job

Set the required environment variables before running the job. Example:

```sh
export DATASTORE_AWS_ACCESS_KEY_ID=your-access-key
export DATASTORE_AWS_SECRET_ACCESS_KEY=your-secret-key
export DATASTORE_S3_BUCKET_NAME=your-bucket
export DATASTORE_S3_ENDPOINT_URL=https://s3.vastdata.example.com
export DATASTORE_S3_REGION=us-east-1
```

### Example spark-submit Command

```sh
spark-submit \
  --class com.acceldata.spark.vast.VastS3EnvJob \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  target/scala-2.12/vasts3-test-job-assembly-0.1.jar
```

**Notes:**
- Adjust the JAR path as per your build output.
- Add any Spark or Hadoop configuration flags as needed for your environment.
- The job does not require command-line arguments; all configuration is via environment variables.

## Output

The job will:
- Print whether GPU resources are detected.
- Write a small DataFrame to the configured S3 bucket.
- Read the data back and print it to stdout.
- Print a success message if all operations succeed.

## Troubleshooting

- Ensure all required environment variables are set and exported in the shell/session that launches `spark-submit`.
- Check that the S3 endpoint and credentials are correct and have appropriate permissions.
- For issues with the S3A connector, ensure the correct Hadoop and AWS SDK versions are on your Spark classpath.

## License

Apache or as specified in this repository.