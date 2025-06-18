package io.acceldata.examples.mysql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Comprehensive MySQL Operations Example
 * 
 * This is the main entry point that demonstrates both read and write operations
 * to MySQL using Apache Spark. It combines examples from MySQLReadExample and
 * MySQLWriteExample to show a complete workflow.
 * 
 * @author Acceldata Platform Team
 * @version 1.0.0
 */
public class MySQLOperationsExample {
    
    private static final Logger logger = LoggerFactory.getLogger(MySQLOperationsExample.class);
    
    // MySQL connection configuration
    private static final String DEFAULT_MYSQL_URL = "jdbc:mysql://mysql-server:3306/acceldata_examples";
    private static final String DEFAULT_MYSQL_USER = "spark_user";
    private static final String DEFAULT_MYSQL_PASSWORD = "spark_password";
    
    public static void main(String[] args) {
        logger.info("=== Acceldata MySQL Operations Example ===");
        logger.info("Company: acceldata.io");
        logger.info("Group: io.acceldata");
        
        // Parse command line arguments or use environment variables
        String mysqlUrl = getMySQLUrl(args);
        String mysqlUser = getMySQLUser();
        String mysqlPassword = getMySQLPassword();
        
        // Initialize Spark with optimized configuration for MySQL
        SparkConf conf = new SparkConf()
                .setAppName("MySQL Operations Example - Acceldata Platform")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.execution.arrow.pyspark.enabled", "true");
        
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        
        try {
            logger.info("Spark Session initialized successfully");
            logger.info("MySQL URL: {}", mysqlUrl);
            logger.info("MySQL User: {}", mysqlUser);
            
            // Create connection properties
            Properties connectionProps = createConnectionProperties(mysqlUser, mysqlPassword);
            
            // Step 1: Setup database and tables
            logger.info("\n=== STEP 1: Setting up Database and Tables ===");
            setupDatabase(spark, mysqlUrl, connectionProps);
            
            // Step 2: Write sample data to MySQL
            logger.info("\n=== STEP 2: Writing Sample Data to MySQL ===");
            runWriteOperations(spark, mysqlUrl, connectionProps);
            
            // Step 3: Read data from MySQL
            logger.info("\n=== STEP 3: Reading Data from MySQL ===");
            runReadOperations(spark, mysqlUrl, connectionProps);
            
            // Step 4: Data transformation and analysis
            logger.info("\n=== STEP 4: Data Transformation and Analysis ===");
            runDataAnalysis(spark, mysqlUrl, connectionProps);
            
            // Step 5: Advanced operations
            logger.info("\n=== STEP 5: Advanced MySQL Operations ===");
            runAdvancedOperations(spark, mysqlUrl, connectionProps);
            
            logger.info("\n=== MySQL Operations Example Completed Successfully ===");
            
        } catch (Exception e) {
            logger.error("Error during MySQL operations", e);
            System.exit(1);
        } finally {
            spark.stop();
            logger.info("Spark Session stopped");
        }
    }
    
    /**
     * Get MySQL URL from command line args or environment
     */
    private static String getMySQLUrl(String[] args) {
        if (args.length > 0) {
            return args[0];
        }
        return System.getenv().getOrDefault("MYSQL_URL", DEFAULT_MYSQL_URL);
    }
    
    /**
     * Get MySQL user from environment
     */
    private static String getMySQLUser() {
        return System.getenv().getOrDefault("MYSQL_USER", DEFAULT_MYSQL_USER);
    }
    
    /**
     * Get MySQL password from environment
     */
    private static String getMySQLPassword() {
        return System.getenv().getOrDefault("MYSQL_PASSWORD", DEFAULT_MYSQL_PASSWORD);
    }
    
    /**
     * Create MySQL connection properties
     */
    private static Properties createConnectionProperties(String user, String password) {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        
        // Connection optimization
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        props.setProperty("serverTimezone", "UTC");
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("cachePrepStmts", "true");
        props.setProperty("prepStmtCacheSize", "250");
        props.setProperty("prepStmtCacheSqlLimit", "2048");
        
        return props;
    }
    
    /**
     * Setup database and create sample tables
     */
    private static void setupDatabase(SparkSession spark, String mysqlUrl, Properties props) {
        try {
            logger.info("Setting up database and tables...");
            
            MySQLWriteExample writeExample = new MySQLWriteExample(spark, mysqlUrl, props);
            writeExample.createSampleTables();
            
            logger.info("✓ Database and tables setup completed");
            
        } catch (Exception e) {
            logger.error("Error setting up database", e);
            throw e;
        }
    }
    
    /**
     * Run write operations to demonstrate data ingestion
     */
    private static void runWriteOperations(SparkSession spark, String mysqlUrl, Properties props) {
        try {
            logger.info("Running write operations...");
            
            MySQLWriteExample writeExample = new MySQLWriteExample(spark, mysqlUrl, props);
            
            // Write sample data
            writeExample.writeEmployeeData();
            writeExample.writeDepartmentData();
            writeExample.writeBatchData();
            writeExample.writeWithUpsert();
            
            logger.info("✓ Sample data written to MySQL");
            
        } catch (Exception e) {
            logger.error("Error in write operations", e);
            throw e;
        }
    }
    
    /**
     * Run read operations to demonstrate data consumption
     */
    private static void runReadOperations(SparkSession spark, String mysqlUrl, Properties props) {
        try {
            logger.info("Running read operations...");
            
            MySQLReadExample readExample = new MySQLReadExample(spark, mysqlUrl, props);
            
            // Read data in different ways
            readExample.readFullTable("employees");
            readExample.readWithQuery("employees", "salary > 75000");
            readExample.readWithPartitioning("employees", "id", 1, 1000, 4);
            readExample.readWithJoin();
            
            logger.info("✓ Data read from MySQL");
            
        } catch (Exception e) {
            logger.error("Error in read operations", e);
            throw e;
        }
    }
    
    /**
     * Demonstrate data analysis capabilities
     */
    private static void runDataAnalysis(SparkSession spark, String mysqlUrl, Properties props) {
        try {
            logger.info("Performing data analysis on MySQL data...");
            
            MySQLReadExample readExample = new MySQLReadExample(spark, mysqlUrl, props);
            
            // Perform various analyses
            readExample.analyzeEmployeeData();
            readExample.calculateDepartmentStatistics();
            readExample.performComplexAnalysis();
            
            logger.info("✓ Data quality checks completed");
            logger.info("✓ Statistical analysis performed");
            logger.info("✓ Data aggregations computed");
            
        } catch (Exception e) {
            logger.error("Error in data analysis", e);
            throw e;
        }
    }
    
    /**
     * Demonstrate advanced MySQL operations
     */
    private static void runAdvancedOperations(SparkSession spark, String mysqlUrl, Properties props) {
        try {
            logger.info("Running advanced MySQL operations...");
            
            MySQLWriteExample writeExample = new MySQLWriteExample(spark, mysqlUrl, props);
            MySQLReadExample readExample = new MySQLReadExample(spark, mysqlUrl, props);
            
            // Advanced operations
            writeExample.performTransactionalWrite();
            readExample.readWithCustomSQL();
            writeExample.optimizeBatchOperations();
            
            logger.info("✓ Advanced operations completed");
            
        } catch (Exception e) {
            logger.error("Error in advanced operations", e);
            throw e;
        }
    }
} 