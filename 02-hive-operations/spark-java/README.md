# Spark Java Hive Operations

This project demonstrates Hive operations using Spark with Kerberos authentication.

## Prerequisites

- Java 11 or higher
- Gradle 8.x
- Kerberos setup
- Hive server access

## Environment Variables

Before running the application, set the following environment variables:

```bash
# Kerberos Configuration
export KERBEROS_KEYTAB_PATH="/path/to/your/keytab/file"
export KERBEROS_PRINCIPAL="your-username@ADSRE.COM"
export HIVE_KERBEROS_PRINCIPAL="hive/_HOST@ADSRE.COM"

# Hive Configuration
export HIVE_JDBC_URL="jdbc:mysql://your-hive-host:3306/hive"
export NAMENODE="your-namenode-host"
export PORT="8020"
```

## Building the Project

```bash
cd 02-hive-operations/spark-java
./gradle clean build
./gradle fatJar

```

## Running the Application

```bash
KERBEROS_KEYTAB_PATH=/path/to/your.keytab \
KERBEROS_PRINCIPAL=your-username@ADSRE.COM \
java -jar ./build/libs/spark-java-1.1.0-all.jar
```

For detailed logging:
```bash
./gradlew runWithSparkArgs --info
```

## Running with Docker

You can build and run this project using Docker, which ensures the correct Java and Spark environment:

### Build Docker Image

```bash
cd 02-hive-operations/spark-java
# Build the Docker image (ensure Docker is running)
docker build -t spark-java-hive:latest .
```

### Run the Application in Docker

```bash
# Run the container, passing required environment variables
# (adjust paths and values as needed)
docker run --rm \
  -e KERBEROS_KEYTAB_PATH="/path/to/your.keytab" \
  -e KERBEROS_PRINCIPAL="your-username@ADSRE.COM" \
  -e HIVE_KERBEROS_PRINCIPAL="hive/_HOST@ADSRE.COM" \
  -e HIVE_JDBC_URL="jdbc:mysql://your-hive-host:3306/hive" \
  -e NAMENODE="your-namenode-host" \
  -e PORT="8020" \
  spark-java-hive:latest
```

- The Dockerfile uses Java 11 for build compatibility.
- The built JAR is copied into a Spark runtime image for execution.
- You can mount volumes or pass additional configs as needed for your environment.

## Project Structure

```
spark-java/
├── src/
│   └── main/
│       └── java/
│           └── com/
│               └── example/
│                   └── spark/
│                       └── hive/
│                           └── HiveKerberosClient.java
├── build.gradle
└── README.md
```

## Features

- Kerberos authentication with automatic renewal
- Hive database and table operations
- Spark SQL integration
- Configurable through environment variables

## Troubleshooting

1. Kerberos Authentication Issues:
   - Verify keytab file exists and is accessible
   - Check Kerberos principal format
   - Ensure valid Kerberos ticket (use `klist` to check)

2. Hive Connection Issues:
   - Verify Hive service is running
   - Check JDBC URL format
   - Ensure network connectivity

3. Spark Configuration:
   - Local mode: `spark.master=local[*]`
   - Cluster mode: Configure appropriate master URL 