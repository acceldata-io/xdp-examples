package io.acceldata.examples.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * HDFS Read Operations Example
 * 
 * This example demonstrates various ways to read data from HDFS using Spark:
 * 1. Reading text files as RDD
 * 2. Reading structured data as DataFrame
 * 3. Reading JSON files
 * 4. Reading Parquet files
 * 
 * @author Acceldata Platform Team
 * @version 1.0.0
 */
public class HDFSReadExample {
    
    private static final Logger logger = LoggerFactory.getLogger(HDFSReadExample.class);
    
    public static void main(String[] args) {
        String hdfsPath = args.length > 0 ? args[0] : "hdfs://localhost:9000/data";
        
        // Initialize Spark with Kubernetes/Docker configuration
        SparkConf conf = new SparkConf()
                .setAppName("HDFS Read Example - Acceldata")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        
        // Configure Hadoop for containerized environment
        configureHadoopForContainer(spark);
        
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        
        try {
            logger.info("Starting HDFS Read Operations Example");
            logger.info("HDFS Path: {}", hdfsPath);
            
            // Example 1: Read text file as RDD
            readTextFileAsRDD(jsc, hdfsPath + "/sample.txt");
            
            // Example 2: Read CSV file as DataFrame
            readCSVAsDataFrame(spark, hdfsPath + "/sample.csv");
            
            // Example 3: Read JSON file as DataFrame
            readJSONAsDataFrame(spark, hdfsPath + "/sample.json");
            
            // Example 4: Read Parquet file as DataFrame
            readParquetAsDataFrame(spark, hdfsPath + "/sample.parquet");
            
            // Example 5: Read multiple files with pattern matching
            readMultipleFiles(spark, hdfsPath + "/logs/*.log");
            
            logger.info("HDFS Read Operations completed successfully");
            
        } catch (Exception e) {
            logger.error("Error during HDFS read operations", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
    
    /**
     * Read text file as RDD and perform basic operations
     */
    private static void readTextFileAsRDD(JavaSparkContext jsc, String filePath) {
        try {
            logger.info("Reading text file as RDD: {}", filePath);
            
            JavaRDD<String> textRDD = jsc.textFile(filePath);
            
            // Basic RDD operations
            long lineCount = textRDD.count();
            logger.info("Total lines in file: {}", lineCount);
            
            // Filter non-empty lines
            JavaRDD<String> nonEmptyLines = textRDD.filter(line -> !line.trim().isEmpty());
            logger.info("Non-empty lines: {}", nonEmptyLines.count());
            
            // Show first 5 lines
            logger.info("First 5 lines:");
            nonEmptyLines.take(5).forEach(line -> logger.info("  {}", line));
            
            // Word count example
            JavaRDD<String> words = textRDD.flatMap(line -> java.util.Arrays.asList(line.split("\\s+")).iterator());
            long wordCount = words.count();
            logger.info("Total words: {}", wordCount);
            
        } catch (Exception e) {
            logger.warn("Could not read text file: {} - {}", filePath, e.getMessage());
        }
    }
    
    /**
     * Read CSV file as DataFrame
     */
    private static void readCSVAsDataFrame(SparkSession spark, String filePath) {
        try {
            logger.info("Reading CSV file as DataFrame: {}", filePath);
            
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                    .csv(filePath);
            
            // Show schema
            logger.info("CSV Schema:");
            df.printSchema();
            
            // Show data
            logger.info("CSV Data (first 10 rows):");
            df.show(10);
            
            // Basic statistics
            logger.info("Row count: {}", df.count());
            logger.info("Column count: {}", df.columns().length);
            
        } catch (Exception e) {
            logger.warn("Could not read CSV file: {} - {}", filePath, e.getMessage());
        }
    }
    
    /**
     * Read JSON file as DataFrame
     */
    private static void readJSONAsDataFrame(SparkSession spark, String filePath) {
        try {
            logger.info("Reading JSON file as DataFrame: {}", filePath);
            
            Dataset<Row> df = spark.read()
                    .option("multiline", "true")
                    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                    .json(filePath);
            
            // Show schema
            logger.info("JSON Schema:");
            df.printSchema();
            
            // Show data
            logger.info("JSON Data (first 10 rows):");
            df.show(10, false);
            
            // Basic statistics
            logger.info("Row count: {}", df.count());
            
        } catch (Exception e) {
            logger.warn("Could not read JSON file: {} - {}", filePath, e.getMessage());
        }
    }
    
    /**
     * Read Parquet file as DataFrame
     */
    private static void readParquetAsDataFrame(SparkSession spark, String filePath) {
        try {
            logger.info("Reading Parquet file as DataFrame: {}", filePath);
            
            Dataset<Row> df = spark.read().parquet(filePath);
            
            // Show schema
            logger.info("Parquet Schema:");
            df.printSchema();
            
            // Show data
            logger.info("Parquet Data (first 10 rows):");
            df.show(10);
            
            // Basic statistics
            logger.info("Row count: {}", df.count());
            logger.info("Partitions: {}", df.rdd().getNumPartitions());
            
        } catch (Exception e) {
            logger.warn("Could not read Parquet file: {} - {}", filePath, e.getMessage());
        }
    }
    
    /**
     * Read multiple files using pattern matching
     */
    private static void readMultipleFiles(SparkSession spark, String pathPattern) {
        try {
            logger.info("Reading multiple files with pattern: {}", pathPattern);
            
            JavaRDD<String> textRDD = spark.sparkContext().textFile(pathPattern, 1).toJavaRDD();
            
            long totalLines = textRDD.count();
            logger.info("Total lines across all matching files: {}", totalLines);
            
            // Show sample data
            if (totalLines > 0) {
                logger.info("Sample lines from multiple files:");
                textRDD.take(5).forEach(line -> logger.info("  {}", line));
            }
            
        } catch (Exception e) {
            logger.warn("Could not read files with pattern: {} - {}", pathPattern, e.getMessage());
        }
    }
    
    /**
     * Configure Hadoop for containerized environment with Kerberos authentication
     */
    private static void configureHadoopForContainer(SparkSession spark) {
        try {
            logger.info("Configuring Hadoop for containerized environment");
            
            Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
            
            // Set HDFS namenode - this will be provided by the data platform
            hadoopConf.set("fs.defaultFS", "hdfs://qenamenode1:8020");
            
            // Configure Hadoop configuration files paths (copied by data platform)
            hadoopConf.addResource(new File("/etc/hadoop/conf/core-site.xml").toURI().toURL());
            hadoopConf.addResource(new File("/etc/hadoop/conf/hdfs-site.xml").toURI().toURL());
            hadoopConf.addResource(new File("/etc/hadoop/conf/hive-site.xml").toURI().toURL());
            
            // Configure Kerberos authentication
            configureKerberosAuthentication(hadoopConf);
            
            logger.info("✓ Hadoop configuration completed for containerized environment");
            
        } catch (Exception e) {
            logger.error("Failed to configure Hadoop for container environment", e);
            throw new RuntimeException("Hadoop configuration failed", e);
        }
    }
    
    /**
     * Configure Kerberos authentication using keytab
     */
    private static void configureKerberosAuthentication(Configuration hadoopConf) throws IOException {
        logger.info("Configuring Kerberos authentication");
        
        // Kerberos configuration
        String principal = "hdfs-adocqecluster@ADSRE.COM";
        String keytabPath = "/etc/user.keytab";
        
        // Verify keytab file exists
        File keytabFile = new File(keytabPath);
        if (!keytabFile.exists()) {
            throw new RuntimeException("Keytab file not found at: " + keytabPath);
        }
        
        // Verify krb5.conf exists
        File krb5File = new File("/etc/krb5.conf");
        if (!krb5File.exists()) {
            throw new RuntimeException("Kerberos configuration file not found at: /etc/krb5.conf");
        }
        
        // Set system property for Kerberos configuration
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        
        // Configure Hadoop for Kerberos
        hadoopConf.set("hadoop.security.authentication", "kerberos");
        hadoopConf.set("hadoop.security.authorization", "true");
        
        // Login using the keytab - this is critical!
        UserGroupInformation.setConfiguration(hadoopConf);
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
        
        logger.info("✓ Kerberos authentication configured successfully");
        logger.info("  Principal: {}", principal);
        logger.info("  Keytab: {}", keytabPath);
        logger.info("  Current user: {}", UserGroupInformation.getCurrentUser());
    }
} 