import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==========================
# Environment Variables
# ==========================
URL = os.environ.get("URL")
KERBEROS_PRINCIPAL = os.environ.get("KERBEROS_PRINCIPAL")
KERBEROS_KEYTAB = os.environ.get("KERBEROS_KEYTAB","/etc/user.keytab")
HDFS_FILE_PATH = os.environ.get("HDFS_FILE_PATH")

# ==========================
# Initialize Spark Session
# ==========================
spark = SparkSession.builder.appName("HDFSReadApplication").config("spark.kerberos.keytab", KERBEROS_KEYTAB).config("spark.kerberos.principal", KERBEROS_PRINCIPAL).getOrCreate()

# ==========================
# Authenticate with Kerberos
# ==========================
spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authentication", "kerberos")
spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authorization", "true")
UserGroupInformation = spark.sparkContext._jvm.org.apache.hadoop.security.UserGroupInformation
UserGroupInformation.setConfiguration(spark.sparkContext._jsc.hadoopConfiguration())
UserGroupInformation.loginUserFromKeytab(KERBEROS_PRINCIPAL, KERBEROS_KEYTAB)

# ==========================
# Build HDFS File Path
# ==========================
file_path = f"{URL}/{HDFS_FILE_PATH}"

# ==========================
# Read Data from HDFS
# ==========================
df = spark.read.csv(file_path, header=True, inferSchema=True)

# ==========================
# Show the data
# ==========================
df.show()