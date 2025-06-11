FROM apache/spark:3.5.5-java17-python3

USER root

# Install pip dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

# Set Spark and Hadoop user
ENV SPARK_USER="185"
ENV HADOOP_USER_NAME="185"

# Copy main application code
COPY hive_operations_example.py /app/
RUN chmod 777 /app

USER 185 