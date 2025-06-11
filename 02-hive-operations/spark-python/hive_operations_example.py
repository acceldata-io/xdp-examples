import os
import time
import threading
import logging
from pyspark.sql import SparkSession

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
KERBEROS_KEYTAB_PATH = os.getenv("KERBEROS_KEYTAB_PATH","/etc/user.keytab")
KERBEROS_PRINCIPAL =  os.getenv("KERBEROS_PRINCIPAL","rahul@ADSRE.COM")
HIVE_JDBC_URL = os.getenv("HIVE_JDBC_URL","jdbc:hive2://qedatanode2:2181,qenamenode2:2181,qenamenode1:2181,qedatanode1:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")  # e.g. "jdbc:hive2://hive-server:10000/default"
namenode = os.getenv("NAMENODE","qenamenode1")
port = os.getenv("PORT","8020")
if not KERBEROS_KEYTAB_PATH or not KERBEROS_PRINCIPAL or not HIVE_JDBC_URL:
    raise ValueError("KERBEROS_KEYTAB_PATH, KERBEROS_PRINCIPAL, and HIVE_JDBC_URL must be set in environment variables.")

class HiveKerberosClient:
    def __init__(self):
        self.logger = logger
        self.spark = SparkSession.builder.enableHiveSupport().config("spark.sql.warehouse.dir", f"hdfs://{namenode}:{port}/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.warehouse.dir", f"hdfs://{namenode}:{port}/user/hive/warehouse") \
        .getOrCreate()
        self._authenticate_kerberos()


    def _authenticate_kerberos(self):
        """Kerberos authentication with background renewal"""
        try:
            self.logger.info("Authenticating with Kerberos...")
            UGI = self.spark.sparkContext._jvm.org.apache.hadoop.security.UserGroupInformation
            UGI.setConfiguration(self.spark.sparkContext._jsc.hadoopConfiguration())
            UGI.loginUserFromKeytab(KERBEROS_PRINCIPAL, KERBEROS_KEYTAB_PATH)

            def renew_auth():
                while True:
                    time.sleep(300)
                    try:
                        UGI.loginUserFromKeytab(KERBEROS_PRINCIPAL, KERBEROS_KEYTAB_PATH)
                    except Exception as e:
                        self.logger.error(f"Auth renewal failed: {e}")

            threading.Thread(target=renew_auth, daemon=True).start()
            self.logger.info("Kerberos authentication successful")
        except Exception as e:
            self.logger.error(f"Kerberos authentication failed: {e}")
            raise

    def run(self):
        db = "sample_db"
        table = "sample_table"
        self.logger.info("Creating database if not exists...")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        self.spark.sql(f"DESCRIBE DATABASE EXTENDED {db};")
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

        self.logger.info("Inserting rows...")
        self.spark.sql(f"INSERT INTO {db}.{table} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        # Wait for insert to finish
        self.spark.sql(f"SELECT COUNT(*) FROM {db}.{table}").collect()

        self.logger.info("Reading rows...")
        df = self.spark.sql(f"SELECT * FROM {db}.{table} LIMIT 5")
        df.show()

        # self.logger.info("Dropping table...")
        # self.spark.sql(f"DROP TABLE {db}.{table}")

        self.logger.info("All operations completed.")

    def stop(self):
        self.spark.stop()

if __name__ == "__main__":
    client = HiveKerberosClient()
    try:
        client.run()
    finally:
        client.stop() 