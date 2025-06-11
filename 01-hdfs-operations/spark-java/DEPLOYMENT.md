# Deployment Guide - HDFS Operations Examples

This guide explains how to deploy the HDFS Operations examples in a containerized environment with the Acceldata data platform.

## Overview

The HDFS Operations examples are designed to run as Docker containers in Kubernetes, with automatic configuration management by the Acceldata data platform.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                       │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Acceldata Data Platform                    │ │
│  │  ┌─────────────────────────────────────────────────────┐ │ │
│  │  │           HDFS Operations Container                 │ │ │
│  │  │                                                     │ │ │
│  │  │  ┌─────────────────────────────────────────────────┐ │ │ │
│  │  │  │              Java Application                   │ │ │ │
│  │  │  │  • HDFSReadExample.java                        │ │ │ │
│  │  │  │  • HDFSWriteExample.java                       │ │ │ │
│  │  │  │  • HDFSOperationsExample.java                  │ │ │ │
│  │  │  └─────────────────────────────────────────────────┘ │ │ │
│  │  │                                                     │ │ │
│  │  │  Configuration Files (mounted by platform):        │ │ │
│  │  │  • /etc/krb5.conf                                  │ │ │
│  │  │  • /etc/hadoop/conf/core-site.xml                 │ │ │
│  │  │  • /etc/hadoop/conf/hdfs-site.xml                 │ │ │
│  │  │  • /etc/hadoop/conf/hive-site.xml                 │ │ │
│  │  │  • /etc/user.keytab                               │ │ │
│  │  └─────────────────────────────────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      HDFS Cluster                          │
│                 hdfs://qenamenode1:8020                     │
│                                                             │
│  Authentication: Kerberos (hdfs-adocqecluster@ADSRE.COM)   │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

1. **Acceldata Data Platform** deployed in Kubernetes
2. **HDFS Cluster** accessible at `hdfs://qenamenode1:8020`
3. **Kerberos Authentication** configured with:
   - Principal: `hdfs-adocqecluster@ADSRE.COM`
   - Keytab file available
4. **Docker Registry** access for image storage

## Build and Deployment Process

### 1. Build the Application

```bash
# Build the fat JAR
./gradlew fatJar

# Build Docker image
./gradlew buildDockerImage

# Push to registry (configure your registry first)
./gradlew pushDockerImage
```

### 2. Container Image Structure

The Docker image contains:
- **Base Image**: `openjdk:11-jre-slim`
- **Application JAR**: `/app/hdfs-operations-examples.jar`
- **Configuration Directories**: `/etc/hadoop/conf` (empty, mounted at runtime)

### 3. Runtime Configuration

The Acceldata data platform automatically provides:

#### Configuration Files
- `/etc/krb5.conf` - Kerberos configuration
- `/etc/hadoop/conf/core-site.xml` - Hadoop core configuration
- `/etc/hadoop/conf/hdfs-site.xml` - HDFS configuration
- `/etc/hadoop/conf/hive-site.xml` - Hive configuration

#### Authentication
- `/etc/user.keytab` - Kerberos keytab for authentication
- Principal: `hdfs-adocqecluster@ADSRE.COM`

#### HDFS Connection
- Namenode: `hdfs://qenamenode1:8020`
- Security: Kerberos authentication enabled

## Application Configuration

### Hadoop Configuration

The application automatically configures Hadoop using the mounted configuration files:

```java
// Set HDFS namenode
hadoopConf.set("fs.defaultFS", "hdfs://qenamenode1:8020");

// Load configuration files
hadoopConf.addResource(new File("/etc/hadoop/conf/core-site.xml").toURI().toURL());
hadoopConf.addResource(new File("/etc/hadoop/conf/hdfs-site.xml").toURI().toURL());
hadoopConf.addResource(new File("/etc/hadoop/conf/hive-site.xml").toURI().toURL());
```

### Kerberos Authentication

The application performs Kerberos authentication on startup:

```java
// Configure Kerberos
String principal = "hdfs-adocqecluster@ADSRE.COM";
String keytabPath = "/etc/user.keytab";

// Login using keytab
UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
```

## Deployment Examples

### 1. Using Acceldata Data Platform

The data platform handles deployment automatically. Simply provide:
- Container image: `acceldata/hdfs-operations-examples:1.0.0`
- Resource requirements
- Job parameters

### 2. Manual Kubernetes Deployment

For reference, see `k8s-deployment.yaml` for a complete Kubernetes deployment example.

### 3. Local Testing

Test the container locally with mounted configuration:

```bash
# Run with mounted configuration files
docker run --rm \
  -v /etc/krb5.conf:/etc/krb5.conf:ro \
  -v /etc/hadoop/conf:/etc/hadoop/conf:ro \
  -v /etc/user.keytab:/etc/user.keytab:ro \
  acceldata/hdfs-operations-examples:1.0.0
```

## Environment Variables

The container supports these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `JAVA_OPTS` | `-Xmx2g -Xms1g` | JVM options |
| `SPARK_CONF_DIR` | `/etc/hadoop/conf` | Spark configuration directory |
| `HADOOP_CONF_DIR` | `/etc/hadoop/conf` | Hadoop configuration directory |
| `KRB5_CONFIG` | `/etc/krb5.conf` | Kerberos configuration file |

## Resource Requirements

### Minimum Requirements
- **Memory**: 1 GB
- **CPU**: 0.5 cores
- **Storage**: 10 GB (for temporary data)

### Recommended Requirements
- **Memory**: 4 GB
- **CPU**: 2 cores
- **Storage**: 50 GB (for data processing)

## Monitoring and Logging

### Health Checks

The container includes health checks:
- **Liveness Probe**: Checks if Java process is running
- **Readiness Probe**: Verifies configuration files are mounted

### Logging

Application logs are written to stdout/stderr and include:
- Hadoop configuration status
- Kerberos authentication status
- HDFS operation results
- Error details and stack traces

### Metrics

The application logs key metrics:
- Data processing times
- File sizes and record counts
- HDFS operation success/failure rates

## Troubleshooting

### Common Issues

1. **Kerberos Authentication Failure**
   ```
   Error: Keytab file not found at: /etc/user.keytab
   ```
   - Verify keytab is mounted correctly
   - Check file permissions (should be 600)

2. **HDFS Connection Issues**
   ```
   Error: Failed to connect to hdfs://qenamenode1:8020
   ```
   - Verify namenode is accessible
   - Check network connectivity
   - Verify Hadoop configuration files

3. **Configuration File Missing**
   ```
   Error: Kerberos configuration file not found at: /etc/krb5.conf
   ```
   - Verify all configuration files are mounted
   - Check ConfigMap/Secret configurations

### Debug Mode

Enable debug logging by setting:
```bash
export JAVA_OPTS="$JAVA_OPTS -Dlog4j.logger.org.apache.hadoop=DEBUG"
```

## Security Considerations

1. **Keytab Security**
   - Keytab files should have 600 permissions
   - Store keytabs in Kubernetes Secrets
   - Rotate keytabs regularly

2. **Network Security**
   - Use network policies to restrict access
   - Enable TLS for HDFS communication
   - Monitor network traffic

3. **Container Security**
   - Run as non-root user (UID 1000)
   - Use read-only root filesystem where possible
   - Scan images for vulnerabilities

## Performance Tuning

### JVM Tuning
```bash
export JAVA_OPTS="-Xmx4g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
```

### Spark Configuration
```bash
export SPARK_OPTS="--conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true"
```

### HDFS Optimization
- Use appropriate block sizes for your data
- Enable compression for better performance
- Partition data appropriately

## Support

For issues related to:
- **Application Code**: Check GitHub repository issues
- **Acceldata Platform**: Contact Acceldata support
- **HDFS/Hadoop**: Check Hadoop documentation and logs 