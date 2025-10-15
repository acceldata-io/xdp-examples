import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==========================
# Environment Variables
# ==========================
HDFS_URL = os.environ.get("URL")
KERBEROS_PRINCIPAL = os.environ.get("KERBEROS_PRINCIPAL")  
KERBEROS_KEYTAB = os.environ.get("KERBEROS_KEYTAB")
HDFS_FILE_OUTPUT_PATH = os.environ.get("HDFS_FILE_OUTPUT_PATH")
HDFS_FILE_PATH = os.environ.get("HDFS_FILE_PATH")

# ==========================
# Initialize Spark Session
# ==========================
spark = SparkSession.builder.appName("HDFSWriteApplication").config("spark.kerberos.keytab", KERBEROS_KEYTAB).config("spark.kerberos.principal", KERBEROS_PRINCIPAL).getOrCreate()

# ==========================
# Authenticate with Kerberos
# ==========================
spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authentication", "kerberos")
spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authorization", "true")
UserGroupInformation = spark.sparkContext._jvm.org.apache.hadoop.security.UserGroupInformation
UserGroupInformation.setConfiguration(spark.sparkContext._jsc.hadoopConfiguration())
UserGroupInformation.loginUserFromKeytab(KERBEROS_PRINCIPAL, KERBEROS_KEYTAB)

      
# ==========================
# Build Input File Path
# ==========================
file_path = f"{HDFS_URL}/{HDFS_FILE_PATH}"

# ==========================
# Read Data from HDFS
# ==========================
df = spark.read.csv(file_path, header=True, inferSchema=True)

# ==========================
# Set Output HDFS File Path
# ==========================
output_path = f"{HDFS_URL}/{HDFS_FILE_OUTPUT_PATH}"

# ==========================
# Write Data to HDFS
# ==========================
df.write.csv(output_path, header=True, mode="overwrite")

df_hdfs = spark.read.csv(output_path, header=True, inferSchema=True)

# ==========================
# Show the data
# ==========================
df_hdfs.show()