# FROM 191579300362.dkr.ecr.us-east-1.amazonaws.com/internal/spark:jdk-3.5.1-2.12-20250604
FROM gradle:8.5.0-jdk11 AS build
# WORKDIR /app
# COPY . .
# RUN gradle build --no-daemon

# Runtime stage
FROM 191579300362.dkr.ecr.us-east-1.amazonaws.com/internal/spark:jdk-3.5.1-2.12-20250604
USER root

# Create directory for the application
WORKDIR /opt/spark/apps

# Copy the built JAR from the build stage
COPY build/libs/spark-java-1.1.0-all.jar /tmp

RUN mkdir -p /opt/spark/conf \
    && chmod 777 /opt/spark/conf \
    && chown 1001:1001 /opt/spark/conf



RUN chmod -R 777 /tmp \
    && chown -R 1001:1001 /tmp

RUN chmod -R 777 /opt/spark
RUN chown -R 1001:1001 /opt/spark


USER 1001
