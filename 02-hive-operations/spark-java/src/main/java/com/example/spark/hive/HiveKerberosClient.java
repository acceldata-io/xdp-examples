package com.example.spark.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HiveKerberosClient {
    
    private static final Logger logger = LoggerFactory.getLogger(HiveKerberosClient.class);
    
    private String keytabPath;
    private String kerberosPrincipal;
    private String hiveKerberosPrincipal;
    private String hiveJdbcUrl;
    private String namenode;
    private String port;
    private SparkSession spark;
    private ScheduledExecutorService authRenewalService;
    private String hadoopConfDir;
    
    public HiveKerberosClient() {
        // Read configuration from environment variables with defaults
        // this.keytabPath = getEnvOrDefault("KERBEROS_KEYTAB_PATH", "/Users/rahulshirgave/Downloads/rahul_shirgave.keytab");
        this.keytabPath = getEnvOrDefault("KERBEROS_KEYTAB_PATH", "/etc/user.keytab");

        this.kerberosPrincipal = getEnvOrDefault("KERBEROS_PRINCIPAL", "rahulshirgave@ADSRE.COM");
        this.hiveKerberosPrincipal = getEnvOrDefault("HIVE_KERBEROS_PRINCIPAL", "hive/_HOST@ADSRE.COM");
        this.hiveJdbcUrl = getEnvOrDefault("HIVE_JDBC_URL", "jdbc:mysql://qenamenode1:3306/hive");
        this.namenode = getEnvOrDefault("NAMENODE", "qenamenode1");
        this.port = getEnvOrDefault("PORT", "8020");
        // this.hadoopConfDir = getEnvOrDefault("HADOOP_CONF_DIR", "/Users/rahulshirgave/Documents/repos/accelqeBitbucket/ad-automation-test/xdp-playgroud/hadoop-conf");
        this.hadoopConfDir=getEnvOrDefault("HADOOP_CONF_DIR","/etc/hadoop/conf");
        logger.info("KERBEROS_KEYTAB_PATH: {}", keytabPath);
        logger.info("KERBEROS_PRINCIPAL: {}", kerberosPrincipal);
        logger.info("HIVE_KERBEROS_PRINCIPAL: {}", hiveKerberosPrincipal);
        logger.info("HADOOP_CONF_DIR: {}", hadoopConfDir);
        
        // Set the hadoop configuration directory system property
        System.setProperty("hadoop.conf.dir", hadoopConfDir);
        
        // Load Hadoop configuration files BEFORE setting up Spark
        loadHadoopConfiguration();

        if (keytabPath == null || kerberosPrincipal == null || hiveJdbcUrl == null) {
            throw new IllegalArgumentException("KERBEROS_KEYTAB_PATH, KERBEROS_PRINCIPAL, and HIVE_JDBC_URL must be set in environment variables.");
        }
        
        setupSparkWithKerberos();
    }
    
    /**
     * Helper method to get environment variable with default value
     */
    private String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.trim().isEmpty()) ? value : defaultValue;
    }
    
    /**
     * Load Hadoop configuration from HADOOP_CONF_DIR
     */
    private void loadHadoopConfiguration() {
        try {
            logger.info("Loading Hadoop configuration from: {}", hadoopConfDir);
            
            // Create a new Hadoop configuration and load the config files
            org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
            
            // Add the configuration files from HADOOP_CONF_DIR
            hadoopConf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDir + "/core-site.xml"));
            hadoopConf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDir + "/hdfs-site.xml"));
            hadoopConf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDir + "/hive-site.xml"));
            
            // Set this configuration as the default for UserGroupInformation
            org.apache.hadoop.security.UserGroupInformation.setConfiguration(hadoopConf);
            
            // Log some key configurations for verification
            logger.info("fs.defaultFS: {}", hadoopConf.get("fs.defaultFS"));
            logger.info("hadoop.security.authentication: {}", hadoopConf.get("hadoop.security.authentication"));
            logger.info("dfs.namenode.kerberos.principal: {}", hadoopConf.get("dfs.namenode.kerberos.principal"));
            logger.info("dfs.datanode.kerberos.principal: {}", hadoopConf.get("dfs.datanode.kerberos.principal"));
            logger.info("hive.metastore.kerberos.principal: {}", hadoopConf.get("hive.metastore.kerberos.principal"));
            
            logger.info("Hadoop configuration loaded successfully");
            
        } catch (Exception e) {
            logger.error("Failed to load Hadoop configuration from HADOOP_CONF_DIR: {}", e.getMessage(), e);
            // Don't throw exception here, let it fall back to manual configuration
        }
    }
    
    /**
     * Set up Spark with proper Kerberos configuration
     */
    private void setupSparkWithKerberos() {
        try {
            logger.info("Setting up Spark with Kerberos authentication...");
            
            // Create Spark configuration with Kerberos settings
            SparkConf conf = new SparkConf()
                .setAppName("HiveKerberosClient")
                // .setMaster("local[*]")  // Set master URL for local execution
                
                // Critical Kerberos configurations
                .set("spark.hadoop.hadoop.security.authentication", "kerberos")
                .set("spark.hadoop.hadoop.security.authorization", "true")
                
                // Hive Metastore configurations
                .set("spark.hadoop.hive.metastore.sasl.enabled", "true")
                .set("spark.hadoop.hive.metastore.kerberos.keytab.file", keytabPath)
                .set("spark.hadoop.hive.metastore.kerberos.principal", hiveKerberosPrincipal)
                
                // HDFS Kerberos principals - required for HDFS connectivity
                .set("spark.hadoop.dfs.namenode.kerberos.principal", "nn/_HOST@ADSRE.COM")
                .set("spark.hadoop.dfs.datanode.kerberos.principal", "dn/_HOST@ADSRE.COM")
                
                // HDFS configuration
                .set("spark.hadoop.fs.defaultFS", "hdfs://" + namenode + ":" + port)
                .set("spark.hadoop.dfs.namenode.rpc-address", namenode + ":" + port)
                .set("spark.hadoop.dfs.client.use.datanode.hostname", "false")
                .set("spark.hadoop.dfs.client.use.legacy.blockreader.local", "false")
                
                // Additional Hive configurations
                .set("spark.sql.warehouse.dir", "hdfs://" + namenode + ":" + port + "/user/rahulshirgave/warehouse")
                .set("spark.hadoop.hive.exec.dynamic.partition", "true")
                .set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
                
                // Serialization settings
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            
            // Create SparkSession with Kerberos-enabled configuration
            this.spark = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
            
            // Debug: Log the final Hadoop configuration that Spark is using
            debugSparkHadoopConfiguration();
            
            // Now authenticate with Kerberos BEFORE any operations
            authenticateKerberos();
            
            logger.info("Spark session created with Kerberos authentication");
            
        } catch (Exception e) {
            logger.error("Failed to setup Spark with Kerberos: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to setup Spark with Kerberos", e);
        }
    }
    
    /**
     * Debug method to show what Hadoop configuration Spark is actually using
     */
    private void debugSparkHadoopConfiguration() {
        try {
            org.apache.hadoop.conf.Configuration sparkHadoopConf = spark.sparkContext().hadoopConfiguration();
            
            logger.info("=== Spark Hadoop Configuration Debug ===");
            logger.info("fs.defaultFS: {}", sparkHadoopConf.get("fs.defaultFS"));
            logger.info("dfs.namenode.rpc-address: {}", sparkHadoopConf.get("dfs.namenode.rpc-address"));
            logger.info("hadoop.security.authentication: {}", sparkHadoopConf.get("hadoop.security.authentication"));
            logger.info("dfs.namenode.kerberos.principal: {}", sparkHadoopConf.get("dfs.namenode.kerberos.principal"));
            logger.info("dfs.datanode.kerberos.principal: {}", sparkHadoopConf.get("dfs.datanode.kerberos.principal"));
            logger.info("hive.metastore.kerberos.principal: {}", sparkHadoopConf.get("hive.metastore.kerberos.principal"));
            logger.info("hive.metastore.sasl.enabled: {}", sparkHadoopConf.get("hive.metastore.sasl.enabled"));
            logger.info("dfs.client.use.datanode.hostname: {}", sparkHadoopConf.get("dfs.client.use.datanode.hostname"));
            logger.info("=== End Debug ===");
            
        } catch (Exception e) {
            logger.error("Failed to debug Spark Hadoop configuration: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Kerberos authentication with background renewal
     */
    private void authenticateKerberos() {
        try {            
            logger.info("Authenticating with Kerberos...");
            
            // Get Hadoop configuration and set security
            org.apache.hadoop.conf.Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
            
            // Set up UserGroupInformation with the Spark's Hadoop configuration
            org.apache.hadoop.security.UserGroupInformation.setConfiguration(hadoopConf);
            
            // Login from keytab
            org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, keytabPath);
            
            // Verify authentication
            org.apache.hadoop.security.UserGroupInformation currentUser = 
                org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
            logger.info("Authenticated as: {}", currentUser.getUserName());
            logger.info("Authentication method: {}", currentUser.getAuthenticationMethod());
            logger.info("Has Kerberos credentials: {}", currentUser.hasKerberosCredentials());
            
            // Test HDFS connectivity
            testHDFSConnection();
            
            // Start background authentication renewal
            startAuthRenewal();
            
            logger.info("Kerberos authentication successful");
            
        } catch (Exception e) {
            logger.error("Kerberos authentication failed: {}", e.getMessage(), e);
            throw new RuntimeException("Kerberos authentication failed", e);
        }
    }
    
    /**
     * Test HDFS connection to verify everything is working
     */
    private void testHDFSConnection() {
        try {
            logger.info("Testing HDFS connection...");
            
            org.apache.hadoop.conf.Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf);
            
            // Test basic operations
            org.apache.hadoop.fs.Path testPath = new org.apache.hadoop.fs.Path("/");
            org.apache.hadoop.fs.FileStatus[] status = fs.listStatus(testPath);
            logger.info("HDFS connection successful. Root directory has {} items", status.length);
            
            // Test warehouse directory access
            org.apache.hadoop.fs.Path warehousePath = new org.apache.hadoop.fs.Path("/user/rahulshirgave/warehouse");
            if (fs.exists(warehousePath)) {
                logger.info("Warehouse directory exists and is accessible: {}", warehousePath);
            } else {
                logger.info("Warehouse directory does not exist, will be created: {}", warehousePath);
            }
            
            fs.close();
            
        } catch (Exception e) {
            logger.error("HDFS connection test failed: {}", e.getMessage(), e);
            // Don't throw exception here, let the database operations handle it
        }
    }
    
    /**
     * Start background thread for authentication renewal
     */
    private void startAuthRenewal() {
        authRenewalService = Executors.newSingleThreadScheduledExecutor();
        
        authRenewalService.scheduleAtFixedRate(() -> {
            try {
                logger.info("Renewing Kerberos authentication...");
                org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, keytabPath);
                logger.info("Kerberos authentication renewed successfully");
            } catch (Exception e) {
                logger.error("Auth renewal failed: {}", e.getMessage(), e);
            }
        }, 5, 5, TimeUnit.MINUTES); // Renew every 5 minutes
    }
    
    /**
     * Run the main database operations
     */
    public void run() {
        String dbName = "sample_spark_java1";
        String tableName = "sample_table";
        
        try {
            logger.info("Creating database if not exists...");
            Dataset<Row> dbs = spark.sql("SHOW DATABASES LIKE '" + dbName + "'");
            if (dbs.count() == 0) {
                spark.sql(String.format("CREATE DATABASE %s", dbName));
                logger.info("Database created: {}", dbName);
            } else {
                logger.info("Database already exists: {}", dbName);
            }
            
            logger.info("Describing database...");
            Dataset<Row> dbDesc = spark.sql(String.format("DESCRIBE DATABASE EXTENDED %s", dbName));
            dbDesc.show();
            
            logger.info("Using database...");
            spark.sql(String.format("USE %s", dbName));
            
            logger.info("Creating table if not exists...");
            Dataset<Row> tables = spark.sql(String.format("SHOW TABLES LIKE '%s'", tableName));
            if (tables.count() == 0) {
                spark.sql(String.format(
                    "CREATE TABLE %s.%s (" +
                    "id INT, " +
                    "name STRING) " +
                    "STORED AS PARQUET",
                    dbName, tableName));
                logger.info("Table created: {}.{}", dbName, tableName);
            } else {
                logger.info("Table already exists: {}.{}", dbName, tableName);
            }
            
            // Wait for table creation to finish
            spark.catalog().refreshTable(String.format("%s.%s", dbName, tableName));
            Dataset<Row> tableDesc = spark.sql(String.format("DESCRIBE EXTENDED %s.%s", dbName, tableName));
            tableDesc.show();
            
            logger.info("Inserting rows...");
            spark.sql(String.format("INSERT INTO %s.%s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", dbName, tableName));
            
            // Wait for insert to finish and get count
            Dataset<Row> countResult = spark.sql(String.format("SELECT COUNT(*) FROM %s.%s", dbName, tableName));
            long count = countResult.collectAsList().get(0).getLong(0);
            logger.info("Table now has {} rows", count);
            
            logger.info("Reading rows...");
            Dataset<Row> df = spark.sql(String.format("SELECT * FROM %s.%s LIMIT 5", dbName, tableName));
            df.show();
            
            // Uncomment to drop table and database
            // spark.sql(String.format("DROP TABLE IF EXISTS %s.%s", dbName, tableName));
            // spark.sql(String.format("DROP DATABASE IF EXISTS %s", dbName));
            
            logger.info("All operations completed successfully.");
            
        } catch (Exception e) {
            logger.error("Database operations failed: {}", e.getMessage(), e);
            throw new RuntimeException("Database operations failed", e);
        }
    }
    
    /**
     * Stop Spark session and cleanup resources
     */
    public void stop() {
        try {
            if (authRenewalService != null && !authRenewalService.isShutdown()) {
                authRenewalService.shutdown();
                authRenewalService.awaitTermination(30, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted while stopping auth renewal service", e);
            Thread.currentThread().interrupt();
        }
        
        if (spark != null) {
            spark.stop();
            logger.info("Spark session stopped");
        }
    }
    
    public static void main(String[] args) {
        HiveKerberosClient client = new HiveKerberosClient();
        
        try {
            client.run();
        } catch (Exception e) {
            logger.error("Application failed: {}", e.getMessage(), e);
            System.exit(1);
        } finally {
            client.stop();
        }
    }
}