# Base image with Python and Spark
ARG BASE_IMAGE=public.ecr.aws/l7l2s8m9/spark-python-jdk-ubuntu:3.5.1-2.12-3.11.13-11.0.27_6-24.04-20250529-20250604

FROM ${BASE_IMAGE}

USER root

WORKDIR /app

ENV SPARK_HOME=/opt/spark
ENV SPARK_JARS_DIR=$SPARK_HOME/jars


# Set environment variables for Kerberos and Matplotlib
ENV HADOOP_CONF_DIR=/etc/hadoop/conf \
    JAVA_SECURITY_KRB5_CONF=/etc/krb5.conf \
    KRB5_CONFIG=/etc/krb5.conf \
    CLASSPATH=$SPARK_JARS_DIR/* \
    PATH=$PATH:$SPARK_JARS_DIR \
    SPARK_USER="185" \
    HADOOP_USER="185"


COPY ODP/ /app/ODP/
COPY S3/ /app/S3/
COPY ../jars/ /opt/spark/jars/
COPY ODP/target/scala-2.12/hdfsoperations_2.12-0.1.jar /opt/spark/jars/
COPY S3/target/scala-2.12/s3operations_2.12-0.1.jar /opt/spark/jars/


# Install pip and create virtual environment for Python packages
RUN apt-get update && apt-get install -y python3-pip python3-venv && \
    python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --upgrade pip 

# Add virtual environment to PATH
ENV PATH="/opt/venv/bin:$PATH"

RUN chmod 777 /app 
RUN chmod 777 /opt/spark/jars


USER 185

