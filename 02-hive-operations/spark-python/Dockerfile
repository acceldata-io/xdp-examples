FROM 191579300362.dkr.ecr.us-east-1.amazonaws.com/internal/spark-python-jdk-ubuntu:3.5.1-2.12-3.11.13-11.0.27_6-24.04-20250529-20250604

USER root

# Install pip dependencies
#COPY requirements.txt /tmp/requirements.txt
#RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

# Set Spark and Hadoop user
ENV SPARK_USER="185"
ENV HADOOP_USER_NAME="185"

# Copy main application code
COPY hive_operations_example.py /app/
RUN chmod 777 /app

USER 185