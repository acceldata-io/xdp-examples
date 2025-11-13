<<<<<<< HEAD
# Dockerfile for HDFS Operations Examples - Acceldata Data Platform
# This Dockerfile packages the JAR file for deployment in Kubernetes

FROM openjdk:11-jre-slim

# Set metadata
LABEL maintainer="Acceldata Platform Team <platform@acceldata.io>"
LABEL version="1.0.0"
LABEL description="HDFS Operations Examples for Acceldata Data Platform"
LABEL company="acceldata.io"

# Create application directory
WORKDIR /app

# Copy the fat JAR file
COPY build/libs/hdfs-operations-examples-fat-1.0.0.jar /app/hdfs-operations-examples.jar

# Create directories for configuration files (will be mounted by data platform)
RUN mkdir -p /etc/hadoop/conf

# Set environment variables
ENV JAVA_OPTS="-Xmx2g -Xms1g"
ENV SPARK_CONF_DIR="/etc/hadoop/conf"

# The data platform will copy these files at runtime:
# /etc/krb5.conf
# /etc/hadoop/conf/core-site.xml
# /etc/hadoop/conf/hdfs-site.xml
# /etc/hadoop/conf/hive-site.xml
# /etc/user.keytab

# Default command - can be overridden by data platform
CMD ["java", "-jar", "/app/hdfs-operations-examples.jar"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD java -cp /app/hdfs-operations-examples.jar io.acceldata.examples.hdfs.HDFSOperationsExample --health-check || exit 1 
=======
FROM 191579300362.dkr.ecr.us-east-1.amazonaws.com/internal/spark:jdk-3.5.1-2.12-20250604
USER root
# Set working directory
WORKDIR /opt/spark/apps

# Copy your JAR (adjust as needed)
COPY build/libs/hdfs-operations-examples-fat-1.6.0.jar /tmp

ENV SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:/tmp/spark-events"

RUN mkdir -p /opt/spark/conf \
    && chmod 777 /opt/spark/conf \
    && chown 1001:1001 /opt/spark/conf



RUN chmod -R 777 /tmp \
    && chown -R 1001:1001 /tmp

RUN chmod -R 777 /opt/spark
RUN chown -R 1001:1001 /opt/spark


USER 1001
>>>>>>> main
