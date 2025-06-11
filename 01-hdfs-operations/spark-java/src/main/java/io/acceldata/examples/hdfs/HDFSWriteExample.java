package io.acceldata.examples.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * HDFS Write Operations Example
 * 
 * This example demonstrates various ways to write data to HDFS using Spark:
 * 1. Writing RDD as text files
 * 2. Writing DataFrame as CSV
 * 3. Writing DataFrame as JSON
 * 4. Writing DataFrame as Parquet
 * 5. Writing with different partitioning strategies
 * 
 * @author Acceldata Platform Team
 * @version 1.0.0
 */
public class HDFSWriteExample {
    
    private static final Logger logger = LoggerFactory.getLogger(HDFSWriteExample.class);
    
    public static void main(String[] args) {
        String hdfsPath = args.length > 0 ? args[0] : "hdfs://localhost:9000/data/output";
        
        // Initialize Spark with Kubernetes/Docker configuration
        SparkConf conf = new SparkConf()
                .setAppName("HDFS Write Example - Acceldata")
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
            logger.info("Starting HDFS Write Operations Example");
            logger.info("Output HDFS Path: {}", hdfsPath);
            
            // Example 1: Write RDD as text file
            writeRDDAsText(jsc, hdfsPath + "/text_output");
            
            // Example 2: Write DataFrame as CSV
            writeDataFrameAsCSV(spark, hdfsPath + "/csv_output");
            
            // Example 3: Write DataFrame as JSON
            writeDataFrameAsJSON(spark, hdfsPath + "/json_output");
            
            // Example 4: Write DataFrame as Parquet
            writeDataFrameAsParquet(spark, hdfsPath + "/parquet_output");
            
            // Example 5: Write with partitioning
            writeWithPartitioning(spark, hdfsPath + "/partitioned_output");
            
            // Example 6: Write with compression
            writeWithCompression(spark, hdfsPath + "/compressed_output");
            
            logger.info("HDFS Write Operations completed successfully");
            
        } catch (Exception e) {
            logger.error("Error during HDFS write operations", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
    
    /**
     * Write RDD as text file
     */
    private static void writeRDDAsText(JavaSparkContext jsc, String outputPath) {
        try {
            logger.info("Writing RDD as text file to: {}", outputPath);
            
            // Create sample data
            List<String> sampleData = Arrays.asList(
                "Welcome to Acceldata HDFS Examples",
                "This is line 2 of our sample data",
                "Spark makes big data processing easy",
                "HDFS provides distributed storage",
                "Data engineering with Acceldata platform"
            );
            
            JavaRDD<String> textRDD = jsc.parallelize(sampleData);
            
            // Add timestamp to each line
            JavaRDD<String> timestampedRDD = textRDD.map(line -> {
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                return timestamp + " - " + line;
            });
            
            // Write to HDFS
            timestampedRDD.saveAsTextFile(outputPath);
            
            logger.info("Successfully wrote {} lines to {}", sampleData.size(), outputPath);
            
        } catch (Exception e) {
            logger.error("Error writing RDD as text: {}", e.getMessage());
        }
    }
    
    /**
     * Write DataFrame as CSV
     */
    private static void writeDataFrameAsCSV(SparkSession spark, String outputPath) {
        try {
            logger.info("Writing DataFrame as CSV to: {}", outputPath);
            
            // Create sample DataFrame
            Dataset<Row> df = createSampleDataFrame(spark);
            
            // Write as CSV with options
            df.coalesce(1)  // Single file output
              .write()
              .mode("overwrite")
              .option("header", "true")
              .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
              .csv(outputPath);
            
            logger.info("Successfully wrote DataFrame as CSV to {}", outputPath);
            
        } catch (Exception e) {
            logger.error("Error writing DataFrame as CSV: {}", e.getMessage());
        }
    }
    
    /**
     * Write DataFrame as JSON
     */
    private static void writeDataFrameAsJSON(SparkSession spark, String outputPath) {
        try {
            logger.info("Writing DataFrame as JSON to: {}", outputPath);
            
            // Create sample DataFrame
            Dataset<Row> df = createSampleDataFrame(spark);
            
            // Write as JSON
            df.coalesce(1)  // Single file output
              .write()
              .mode("overwrite")
              .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
              .json(outputPath);
            
            logger.info("Successfully wrote DataFrame as JSON to {}", outputPath);
            
        } catch (Exception e) {
            logger.error("Error writing DataFrame as JSON: {}", e.getMessage());
        }
    }
    
    /**
     * Write DataFrame as Parquet
     */
    private static void writeDataFrameAsParquet(SparkSession spark, String outputPath) {
        try {
            logger.info("Writing DataFrame as Parquet to: {}", outputPath);
            
            // Create sample DataFrame
            Dataset<Row> df = createSampleDataFrame(spark);
            
            // Write as Parquet
            df.write()
              .mode("overwrite")
              .parquet(outputPath);
            
            logger.info("Successfully wrote DataFrame as Parquet to {}", outputPath);
            
        } catch (Exception e) {
            logger.error("Error writing DataFrame as Parquet: {}", e.getMessage());
        }
    }
    
    /**
     * Write with partitioning strategy
     */
    private static void writeWithPartitioning(SparkSession spark, String outputPath) {
        try {
            logger.info("Writing DataFrame with partitioning to: {}", outputPath);
            
            // Create sample DataFrame with partition columns
            Dataset<Row> df = createSampleDataFrameWithPartitions(spark);
            
            // Write with partitioning
            df.write()
              .mode("overwrite")
              .partitionBy("department", "year")
              .parquet(outputPath);
            
            logger.info("Successfully wrote partitioned DataFrame to {}", outputPath);
            
        } catch (Exception e) {
            logger.error("Error writing partitioned DataFrame: {}", e.getMessage());
        }
    }
    
    /**
     * Write with compression
     */
    private static void writeWithCompression(SparkSession spark, String outputPath) {
        try {
            logger.info("Writing DataFrame with compression to: {}", outputPath);
            
            // Create sample DataFrame
            Dataset<Row> df = createSampleDataFrame(spark);
            
            // Write with GZIP compression
            df.write()
              .mode("overwrite")
              .option("compression", "gzip")
              .parquet(outputPath);
            
            logger.info("Successfully wrote compressed DataFrame to {}", outputPath);
            
        } catch (Exception e) {
            logger.error("Error writing compressed DataFrame: {}", e.getMessage());
        }
    }
    
    /**
     * Create sample DataFrame for examples
     */
    private static Dataset<Row> createSampleDataFrame(SparkSession spark) {
        // Define schema
        StructType schema = new StructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("email", DataTypes.StringType, false),
            DataTypes.createStructField("salary", DataTypes.DoubleType, false),
            DataTypes.createStructField("created_at", DataTypes.TimestampType, false)
        });
        
        // Create sample data
        List<Row> data = Arrays.asList(
            RowFactory.create(1, "John Doe", "john.doe@acceldata.io", 75000.0, 
                java.sql.Timestamp.valueOf("2023-01-15 10:30:00")),
            RowFactory.create(2, "Jane Smith", "jane.smith@acceldata.io", 82000.0, 
                java.sql.Timestamp.valueOf("2023-02-20 14:15:00")),
            RowFactory.create(3, "Bob Johnson", "bob.johnson@acceldata.io", 68000.0, 
                java.sql.Timestamp.valueOf("2023-03-10 09:45:00")),
            RowFactory.create(4, "Alice Brown", "alice.brown@acceldata.io", 91000.0, 
                java.sql.Timestamp.valueOf("2023-04-05 16:20:00")),
            RowFactory.create(5, "Charlie Wilson", "charlie.wilson@acceldata.io", 77500.0, 
                java.sql.Timestamp.valueOf("2023-05-12 11:10:00"))
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    /**
     * Create sample DataFrame with partition columns
     */
    private static Dataset<Row> createSampleDataFrameWithPartitions(SparkSession spark) {
        // Define schema with partition columns
        StructType schema = new StructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("department", DataTypes.StringType, false),
            DataTypes.createStructField("salary", DataTypes.DoubleType, false),
            DataTypes.createStructField("year", DataTypes.IntegerType, false)
        });
        
        // Create sample data with different departments and years
        Random random = new Random();
        String[] departments = {"Engineering", "Sales", "Marketing", "HR"};
        int[] years = {2022, 2023, 2024};
        
        List<Row> data = Arrays.asList(
            RowFactory.create(1, "John Doe", departments[0], 75000.0 + random.nextInt(20000), years[1]),
            RowFactory.create(2, "Jane Smith", departments[1], 82000.0 + random.nextInt(20000), years[1]),
            RowFactory.create(3, "Bob Johnson", departments[0], 68000.0 + random.nextInt(20000), years[2]),
            RowFactory.create(4, "Alice Brown", departments[2], 91000.0 + random.nextInt(20000), years[2]),
            RowFactory.create(5, "Charlie Wilson", departments[3], 77500.0 + random.nextInt(20000), years[1]),
            RowFactory.create(6, "Diana Prince", departments[0], 85000.0 + random.nextInt(20000), years[2]),
            RowFactory.create(7, "Frank Miller", departments[1], 72000.0 + random.nextInt(20000), years[0]),
            RowFactory.create(8, "Grace Lee", departments[2], 88000.0 + random.nextInt(20000), years[0])
        );
        
        return spark.createDataFrame(data, schema);
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