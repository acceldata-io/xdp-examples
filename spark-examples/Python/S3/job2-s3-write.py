from pyspark.sql import SparkSession
import os

# ==========================
# Spark Session
# ==========================
spark = SparkSession.builder \
    .appName("ReadFromS3") \
    .getOrCreate()

# ==========================
# S3 Configuration
# ==========================
access_key = os.environ.get("DATASTORE_AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("DATASTORE_AWS_SECRET_ACCESS_KEY")
bucket_name = os.environ.get("DATASTORE_S3_BUCKET_NAME")
s3_file_path =os.environ.get("DATASTORE_S3_FILE_PATH")
s3_file_path_output =os.environ.get("DATASTORE_S3_FILE_PATH_OUTPUT")
s3_region = os.environ.get("DATASTORE_S3_REGION")



# Set S3 credentials in Hadoop configuration
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)

# Optional: specify S3 endpoint if using non-AWS S3
# hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

# ==========================
# Read data from S3
# ==========================
df = spark.read.csv(f"s3a://{bucket_name}/{s3_file_path}", header=True, inferSchema=True)

# Show sample
df.show()

# ==========================
# Write data to S3
# ==========================
df.write.csv(f"s3a://{bucket_name}/{s3_file_path_output}", header=True, mode="overwrite")

# ==========================
# Read data from S3
# ==========================
df = spark.read.csv(f"s3a://{bucket_name}/{s3_file_path_output}", header=True, inferSchema=True)


# Stop Spark
spark.stop()