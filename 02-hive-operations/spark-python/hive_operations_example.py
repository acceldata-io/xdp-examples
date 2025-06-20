import os
import time
import threading
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

# Logger setup
def setup_logger():
    logger = logging.getLogger("HiveKerberosApp")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

logger = setup_logger()

# Read Kerberos and JDBC details from environment
KERBEROS_KEYTAB_PATH = os.getenv("KERBEROS_KEYTAB_PATH", "/etc/user.keytab")

KERBEROS_PRINCIPAL = os.getenv("KERBEROS_PRINCIPAL", "rahulshirgave@ADSRE.COM")
HIVE_KERBEROS_PRINCIPAL = os.getenv("HIVE_KERBEROS_PRINCIPAL", "hive/_HOST@ADSRE.COM")
logger.info(f"KERBEROS_KEYTAB_PATH: {KERBEROS_KEYTAB_PATH}")
logger.info(f"KERBEROS_PRINCIPAL: {KERBEROS_PRINCIPAL}")
namenode = os.getenv("NAMENODE", "qenamenode1")
port = os.getenv("PORT", "8020")
HIVE_JDBC_URL = os.getenv("HIVE_JDBC_URL", f"jdbc:mysql://${namenode}:${port}/hive")


if not KERBEROS_KEYTAB_PATH or not KERBEROS_PRINCIPAL or not HIVE_JDBC_URL:
    raise ValueError("KERBEROS_KEYTAB_PATH, KERBEROS_PRINCIPAL, and HIVE_JDBC_URL must be set in environment variables.")

class HiveKerberosClient:
    def __init__(self):
        self.logger = logger
        self.spark = None
        self._setup_spark_with_kerberos()

    def _setup_spark_with_kerberos(self):
        """Set up Spark with proper Kerberos configuration"""
        try:
            self.logger.info("Setting up Spark with Kerberos authentication...")

            # Create Spark configuration with Kerberos settings
            conf = SparkConf()

            # Critical Kerberos configurations
            # conf.set("spark.sql.warehouse.dir","hdfs://qenamenode1:8020/user/rahulshirgave/warehouse")  # Use HDFS path for Hive warehouse
            conf.set("spark.hadoop.hadoop.security.authentication", "kerberos")
            conf.set("spark.hadoop.hadoop.security.authorization", "true")
            conf.set("spark.hadoop.hive.metastore.sasl.enabled", "true")
            conf.set("spark.hadoop.hive.metastore.kerberos.keytab.file", KERBEROS_KEYTAB_PATH)
            conf.set("spark.hadoop.hive.metastore.kerberos.principal", HIVE_KERBEROS_PRINCIPAL)


            # Create SparkSession with Kerberos-enabled configuration
            self.spark = SparkSession.builder \
                .config(conf=conf) \
                .enableHiveSupport() \
                .getOrCreate()

            # Now authenticate with Kerberos BEFORE any operations
            self._authenticate_kerberos()

            self.logger.info("Spark session created with Kerberos authentication")

        except Exception as e:
            self.logger.error(f"Failed to setup Spark with Kerberos: {e}")
            raise

    def _authenticate_kerberos(self):
        """Kerberos authentication with background renewal"""
        try:
            self.logger.info("Authenticating with Kerberos...")

            # Get Hadoop configuration and set security
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            #hadoop_conf.set("hadoop.security.authentication", "kerberos")
            #hadoop_conf.set("hadoop.security.authorization", "true")

            # Set up UserGroupInformation
            UGI = self.spark.sparkContext._jvm.org.apache.hadoop.security.UserGroupInformation
            UGI.setConfiguration(hadoop_conf)

            # Login from keytab
            UGI.loginUserFromKeytab(KERBEROS_PRINCIPAL, KERBEROS_KEYTAB_PATH)

            # Verify authentication
            current_user = UGI.getCurrentUser()
            self.logger.info(f"Authenticated as: {current_user.getUserName()}")
            self.logger.info(f"Authentication method: {current_user.getAuthenticationMethod()}")

            def renew_auth():
                while True:
                    time.sleep(300)  # Renew every 5 minutes
                    try:
                        self.logger.info("Renewing Kerberos authentication...")
                        UGI.loginUserFromKeytab(KERBEROS_PRINCIPAL, KERBEROS_KEYTAB_PATH)
                        self.logger.info("Kerberos authentication renewed successfully")
                    except Exception as e:
                        self.logger.error(f"Auth renewal failed: {e}")

            threading.Thread(target=renew_auth, daemon=True).start()
            self.logger.info("Kerberos authentication successful")

        except Exception as e:
            self.logger.error(f"Kerberos authentication failed: {e}")
            raise

    def run(self):
        db = "sample_db_hive_python1"
        table = "sample_table"

        try:
            self.logger.info("Creating database if not exists...")
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

            self.logger.info("Describing database...")
            self.spark.sql(f"DESCRIBE DATABASE EXTENDED {db}").show()

            self.logger.info("Using database...")
            self.spark.sql(f"USE {db}")

            self.logger.info("Creating table if not exists...")
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {db}.{table} (
                    id INT,
                    name STRING
                )
                STORED AS PARQUET
            """)

            # Wait for table creation to finish
            self.spark.catalog.refreshTable(f"{db}.{table}")
            self.spark.sql(f"DESCRIBE EXTENDED {db}.{table}").show()
            self.logger.info("Inserting rows...")
            self.spark.sql(f"INSERT INTO {db}.{table} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

            # Wait for insert to finish
            count = self.spark.sql(f"SELECT COUNT(*) FROM {db}.{table}").collect()[0][0]
            self.logger.info(f"Table now has {count} rows")

            self.logger.info("Reading rows...")
            df = self.spark.sql(f"SELECT * FROM {db}.{table} LIMIT 5")
            df.show()
            # drop table and database
            # self.spark.sql(f"DROP TABLE IF EXISTS {db}.{table}").show()
            # self.spark.sql(f"DROP DATABASE IF EXISTS {db}").show()
            self.logger.info("All operations completed successfully.")


        except Exception as e:
            self.logger.error(f"Database operations failed: {e}")
            raise

    def stop(self):
        if self.spark:
            self.spark.stop()

if __name__ == "__main__":
    client = HiveKerberosClient()
    try:
        client.run()
    finally:
        client.stop()