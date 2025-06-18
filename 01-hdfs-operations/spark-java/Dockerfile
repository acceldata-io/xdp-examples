FROM 191579300362.dkr.ecr.us-east-1.amazonaws.com/internal/spark:jdk-3.5.1-2.12-20250604

# Set working directory
WORKDIR /opt/app

# Copy your JAR (adjust as needed)
COPY build/libs/hdfs-operations-examples-fat-1.0.0.jar HDFSReadExample.jar


# Run the application
ENTRYPOINT ["java", "-jar", "HDFSReadExample.jar"]