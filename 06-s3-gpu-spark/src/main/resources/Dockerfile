# Base image with Spark 3.5.1, Scala 2.12, JDK 11
FROM 191579300362.dkr.ecr.us-east-1.amazonaws.com/internal/spark-python-jdk-ubuntu:3.5.1-2.12-3.11.13-11.0.27_6-24.04-20250529-20250604
USER root

# Spark application directory
WORKDIR /opt/spark/apps

# Copy fat/assembly JAR
COPY vasts3-test-job-assembly-0.1.jar /tmp
COPY rapids-4-spark_2.12-25.06.0.3.3.6.2-1-cuda11.jar /opt/spark/jars/

# Copy VAST DB JARS
RUN mkdir -p /opt/spark/vast/jars /tmp/vast_bundle

COPY vast_bundle/ /tmp/vast_bundle/
RUN cp /tmp/vast_bundle/*.jar /opt/spark/vast/jars/

RUN chmod 777 /opt/spark/vast/jars \
    && chown 185:185 /opt/spark/vast/jars

# Make spark conf directory writable
RUN mkdir -p /opt/spark/conf \
    && chmod 777 /opt/spark/conf \
    && chown 185:185 /opt/spark/conf

RUN chmod -R 777 /tmp \
    && chown -R 185:185 /tmp

RUN chmod -R 777 /opt/spark
RUN chown -R 185:185 /opt/spark

# Set user to spark (1001) for runtime
USER 185