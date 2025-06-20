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