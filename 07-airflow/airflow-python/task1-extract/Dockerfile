# Task 1: Extract - Docker Image
# Extracts weather data from API and writes to ADLS staging
# Run via spark-submit: local:///app/main.py

FROM public.ecr.aws/l7l2s8m9/spark-python-jdk-ubuntu:3.5.5.3.3.6.2-1-ubuntu-20

LABEL maintainer="Acceldata Platform Team <platform@acceldata.io>"
LABEL version="5.0.0"
LABEL description="Task 1: Extract weather data to ADLS staging"
LABEL company="acceldata.io"

# Copy requirements and install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code to /app directory
COPY main.py /app/

# Set working directory
WORKDIR /opt/spark/work-dir
