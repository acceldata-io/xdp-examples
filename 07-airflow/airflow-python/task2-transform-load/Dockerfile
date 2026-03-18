# Task 2: Transform & Load - Docker Image
# Transforms staged data and loads to ADLS as Parquet
# Run via spark-submit: local:///app/main.py

FROM public.ecr.aws/l7l2s8m9/spark-python-jdk-ubuntu:3.5.5.3.3.6.2-1-ubuntu-20

LABEL maintainer="Acceldata Platform Team <platform@acceldata.io>"
LABEL version="5.0.0"
LABEL description="Task 2: Transform and load weather data to ADLS"
LABEL company="acceldata.io"

# Copy requirements and install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code to /app directory
COPY main.py /app/

# Set working directory
WORKDIR /opt/spark/work-dir

# No ENTRYPOINT - use spark-submit via SparkApplication
# mainApplicationFile: local:///app/main.py
