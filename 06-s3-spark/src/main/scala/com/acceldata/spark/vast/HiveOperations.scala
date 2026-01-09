package com.acceldata.spark.vast

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Hive Operations using Apache Spark (Scala) with Kerberos Authentication
 *
 * This class supports list, create, insert, and read operations on Hive tables.
 *
 * Usage:
 *   HiveOperations <operation> <database> [table] [options]
 *
 * Operations:
 *   list     <database>                    - List all tables in database
 *   create   <database> <table>            - Create a sample table
 *   insert   <database> <table>            - Insert sample data into table
 *   read     <database> <table>            - Read data from table
 *   describe <database> [table]            - Describe database or table
 *   drop     <database> <table>            - Drop a table
 *
 * Required Environment Variables:
 *   - KERBEROS_KEYTAB_PATH or KERBEROS_KEYTAB (required)
 *   - HIVE_CONF_DIR (required)
 *   - KERBEROS_PRINCIPAL (required)
 *   - HIVE_KERBEROS_PRINCIPAL (optional)
 *
 * Author: Acceldata Platform Team
 * Company: acceldata.io
 */
object HiveOperations {

  def main(args: Array[String]): Unit = {
    // ==========================
    // Parse Command-Line Arguments
    // ==========================
    if (args.length < 2) {
      throw new RuntimeException(
        """Usage: HiveOperations <operation> <database> [table] [options]
          |
          |Operations:
          |  list     <database>              - List all tables in database
          |  create   <database> <table>      - Create a sample table
          |  insert   <database> <table>      - Insert sample data into table
          |  read     <database> <table>      - Read data from table
          |  describe <database> [table]      - Describe database or table
          |  drop     <database> <table>      - Drop a table
          |
          |Examples:
          |  HiveOperations list default
          |  HiveOperations create mydb sample_table
          |  HiveOperations insert mydb sample_table
          |  HiveOperations read mydb sample_table
          |  HiveOperations describe mydb
          |  HiveOperations describe mydb sample_table
          |  HiveOperations drop mydb sample_table
          |""".stripMargin)
    }

    val operation = args(0).toLowerCase
    val database = args(1)
    val table = if (args.length > 2) args(2) else ""

    // ==========================
    // Environment Variables passed in xDP session
    // ==========================
    val keytabPathRaw = getEnvWithFallback("KERBEROS_KEYTAB_PATH", "KERBEROS_KEYTAB", "")
    val keytabPath = stripLocalPrefix(keytabPathRaw)
    val hadoopConfDir = sys.env.getOrElse("HIVE_CONF_DIR", "/etc/hadoop/conf")
    val kerberosPrincipal = sys.env.getOrElse("KERBEROS_PRINCIPAL", "")
    val hiveKerberosPrincipal = sys.env.getOrElse("HIVE_KERBEROS_PRINCIPAL", "hive/_HOST@REALM")
    val hiveMetastoreUri = sys.env.getOrElse("HIVE_METASTORE_URI", "")

    val separator = "=" * 60
    println(separator)
    println("Hive Operations - Acceldata")
    println(separator)
    println(s"Operation: $operation")
    println(s"Database: $database")
    println(s"Table: ${if (table.nonEmpty) table else "<not specified>"}")
    println(s"Kerberos Principal: $kerberosPrincipal")
    println(s"Kerberos Keytab: $keytabPath")
    println(s"Hive Kerberos Principal: $hiveKerberosPrincipal")
    println(s"Hive Metastore URI: ${if (hiveMetastoreUri.nonEmpty) hiveMetastoreUri else "<not set - will use hive-site.xml>"}")
    println(s"Hadoop Conf Dir: $hadoopConfDir")
    println(separator)

    // ==========================
    // Load Hadoop Configuration
    // ==========================
    val hadoopConf = loadHadoopConfiguration(hadoopConfDir)

    // ==========================
    // Authenticate with Kerberos (if configured)
    // ==========================
    if (kerberosPrincipal.nonEmpty && keytabPath.nonEmpty) {
      authenticateKerberos(hadoopConf, kerberosPrincipal, keytabPath)
    } else {
      println("Skipping Kerberos authentication (no principal/keytab provided)")
    }

    // ==========================
    // Create Spark Session with Hive Support
    // ==========================
    val spark = createSparkSession(keytabPathRaw, kerberosPrincipal, hiveKerberosPrincipal, hiveMetastoreUri)

    try {
      // Validate operation requirements
      val requiresTable = Seq("create", "insert", "read", "drop")
      if (requiresTable.contains(operation) && table.isEmpty) {
        throw new RuntimeException(s"Operation '$operation' requires a table name")
      }

      operation match {
        case "list" =>
          listTables(spark, database)

        case "create" =>
          createTable(spark, database, table)

        case "insert" =>
          insertData(spark, database, table)

        case "read" =>
          readData(spark, database, table)

        case "describe" =>
          if (table.nonEmpty) {
            describeTable(spark, database, table)
          } else {
            describeDatabase(spark, database)
          }

        case "drop" =>
          dropTable(spark, database, table)

        case _ =>
          throw new RuntimeException(s"Unknown operation: $operation. Supported: list, create, insert, read, describe, drop")
      }

      println(separator)
      println(s"Operation '$operation' completed successfully!")
      println(separator)

    } catch {
      case e: Exception =>
        println(s"Error during $operation operation: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }

  // ==========================
  // Spark Session Creation
  // ==========================
  private def createSparkSession(
    keytabPath: String,
    kerberosPrincipal: String,
    hiveKerberosPrincipal: String,
    hiveMetastoreUri: String,
  ): SparkSession = {
    println("Creating Spark session with Hive support...")

    val builder = SparkSession.builder()
      .appName("HiveOperations - Acceldata")
      .config("spark.hadoop.hadoop.security.authentication", "kerberos")
      .config("spark.hadoop.hadoop.security.authorization", "true")

    // Add Hive metastore URI if provided (CRITICAL for connecting to actual Hive)
    if (hiveMetastoreUri.nonEmpty) {
      println(s"Configuring Hive metastore URI: $hiveMetastoreUri")
      builder
        .config("hive.metastore.uris", hiveMetastoreUri)
        .config("spark.hadoop.hive.metastore.uris", hiveMetastoreUri)
        .config("spark.sql.hive.metastore.version", "3.1.3")
        .config("spark.sql.hive.metastore.jars", "builtin")
    }

    // Add Kerberos configs if provided
    if (kerberosPrincipal.nonEmpty && keytabPath.nonEmpty) {
      builder
        .config("spark.kerberos.keytab", keytabPath)
        .config("spark.kerberos.principal", kerberosPrincipal)
        .config("spark.hadoop.hive.metastore.sasl.enabled", "true")
        .config("spark.hadoop.hive.metastore.kerberos.keytab.file", stripLocalPrefix(keytabPath))
        .config("spark.hadoop.hive.metastore.kerberos.principal", hiveKerberosPrincipal)
    }

    val spark = builder
      .enableHiveSupport()
      .getOrCreate()

    // After SparkSession is created, verify Hive configuration
    val hiveMetastoreUriConfig = spark.conf.getOption("spark.hadoop.hive.metastore.uris")
      .orElse(spark.conf.getOption("hive.metastore.uris"))
      .getOrElse("<not set>")
    println(s"Effective Hive Metastore URI: $hiveMetastoreUriConfig")

    println("Spark session created with Hive support")
    spark
  }

  // ==========================
  // Load Hadoop Configuration
  // ==========================
  private def loadHadoopConfiguration(hadoopConfDir: String): Configuration = {
    println(s"Loading Hadoop configuration from: $hadoopConfDir")

    val hadoopConf = new Configuration()

    try {
      val coreSite = new Path(s"$hadoopConfDir/core-site.xml")
      val hdfsSite = new Path(s"$hadoopConfDir/hdfs-site.xml")
      val hiveSite = new Path(s"$hadoopConfDir/hive-site.xml")

      hadoopConf.addResource(coreSite)
      hadoopConf.addResource(hdfsSite)
      hadoopConf.addResource(hiveSite)

      println(s"  fs.defaultFS: ${hadoopConf.get("fs.defaultFS", "<not set>")}")
      println(s"  hadoop.security.authentication: ${hadoopConf.get("hadoop.security.authentication", "<not set>")}")
      println("Hadoop configuration loaded successfully")
    } catch {
      case e: Exception =>
        println(s"Warning: Could not load Hadoop configuration: ${e.getMessage}")
    }

    hadoopConf
  }

  // ==========================
  // Kerberos Authentication
  // ==========================
  private def authenticateKerberos(hadoopConf: Configuration, principal: String, keytab: String): Unit = {
    println(s"Authenticating with Kerberos as $principal...")

    hadoopConf.set("hadoop.security.authentication", "kerberos")
    hadoopConf.set("hadoop.security.authorization", "true")

    UserGroupInformation.setConfiguration(hadoopConf)
    UserGroupInformation.loginUserFromKeytab(principal, keytab)

    val currentUser = UserGroupInformation.getCurrentUser
    println(s"Authenticated as: ${currentUser.getUserName}")
    println(s"Authentication method: ${currentUser.getAuthenticationMethod}")
    println(s"Has Kerberos credentials: ${currentUser.hasKerberosCredentials}")
    println("Kerberos authentication successful")
  }

  // ==========================
  // LIST Tables Operation
  // ==========================
  private def listTables(spark: SparkSession, database: String): Unit = {
    println(s"\n=== Listing tables in database: $database ===")

    // Create database if not exists
    //spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.sql(s"USE $database")

    val tables = spark.sql("SHOW TABLES")
    val tableCount = tables.count()

    println(s"\nFound $tableCount tables in database '$database':")
    println("-" * 60)

    if (tableCount > 0) {
      tables.show(100, truncate = false)
    } else {
      println("(no tables found)")
    }

    // Also show databases
    println(s"\nAll databases:")
    spark.sql("SHOW DATABASES").show(50, truncate = false)
  }

  // ==========================
  // CREATE Table Operation
  // ==========================
  private def createTable(spark: SparkSession, database: String, table: String): Unit = {
    println(s"\n=== Creating table: $database.$table ===")

    // Create database if not exists
    //spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.sql(s"USE $database")

    // Check if table exists
    val tables = spark.sql(s"SHOW TABLES LIKE '$table'")
    if (tables.count() > 0) {
      println(s"Table $database.$table already exists")
      describeTable(spark, database, table)
      return
    }

    // Create table
    spark.sql(
      s"""
         |CREATE TABLE $database.$table (
         |  id INT,
         |  name STRING,
         |  department STRING,
         |  salary DOUBLE,
         |  created_date STRING
         |)
         |STORED AS PARQUET
         |""".stripMargin)

    println(s"Table $database.$table created successfully")

    // Describe the created table
    describeTable(spark, database, table)
  }

  // ==========================
  // INSERT Data Operation
  // ==========================
  private def insertData(spark: SparkSession, database: String, table: String): Unit = {
    println(s"\n=== Inserting data into: $database.$table ===")

    spark.sql(s"USE $database")

    // Check if table exists
    val tables = spark.sql(s"SHOW TABLES LIKE '$table'")
    if (tables.count() == 0) {
      println(s"Table $database.$table does not exist. Creating it first...")
      createTable(spark, database, table)
    }

    // Get current timestamp for created_date
    val timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date())

    // Insert sample data
    spark.sql(
      s"""
         |INSERT INTO $database.$table VALUES
         |  (1, 'Alice', 'Engineering', 75000.0, '$timestamp'),
         |  (2, 'Bob', 'Marketing', 65000.0, '$timestamp'),
         |  (3, 'Charlie', 'Engineering', 80000.0, '$timestamp'),
         |  (4, 'Diana', 'Sales', 70000.0, '$timestamp'),
         |  (5, 'Eve', 'Marketing', 72000.0, '$timestamp')
         |""".stripMargin)

    println("Sample data inserted successfully")

    // Show count
    val count = spark.sql(s"SELECT COUNT(*) as total FROM $database.$table").collect()(0).getLong(0)
    println(s"Table now has $count rows")

    // Show sample data
    println("\nData preview:")
    spark.sql(s"SELECT * FROM $database.$table ORDER BY id LIMIT 10").show(truncate = false)
  }

  // ==========================
  // READ Data Operation
  // ==========================
  private def readData(spark: SparkSession, database: String, table: String): Unit = {
    println(s"\n=== Reading data from: $database.$table ===")

    spark.sql(s"USE $database")

    // Check if table exists
    val tables = spark.sql(s"SHOW TABLES LIKE '$table'")
    if (tables.count() == 0) {
      println(s"Table $database.$table does not exist")
      return
    }

    // Get count
    val count = spark.sql(s"SELECT COUNT(*) as total FROM $database.$table").collect()(0).getLong(0)
    println(s"Total rows: $count")

    // Show schema
    println("\nSchema:")
    val df = spark.table(s"$database.$table")
    df.printSchema()

    // Show data
    println(s"\nData (showing up to 50 rows):")
    df.show(50, truncate = false)

    // Show statistics for numeric columns
    println("\nNumeric column statistics:")
    df.describe("id", "salary").show()

    // Group by department
    println("\nRows by department:")
    spark.sql(
      s"""
         |SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
         |FROM $database.$table
         |GROUP BY department
         |ORDER BY count DESC
         |""".stripMargin).show(truncate = false)
  }

  // ==========================
  // DESCRIBE Database/Table Operation
  // ==========================
  private def describeDatabase(spark: SparkSession, database: String): Unit = {
    println(s"\n=== Describing database: $database ===")

    // Create database if not exists
   // spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")

    println("\nDatabase details:")
    spark.sql(s"DESCRIBE DATABASE EXTENDED $database").show(truncate = false)

    println("\nTables in database:")
    spark.sql(s"USE $database")
    spark.sql("SHOW TABLES").show(100, truncate = false)
  }

  private def describeTable(spark: SparkSession, database: String, table: String): Unit = {
    println(s"\n=== Describing table: $database.$table ===")

    spark.sql(s"USE $database")

    // Check if table exists
    val tables = spark.sql(s"SHOW TABLES LIKE '$table'")
    if (tables.count() == 0) {
      println(s"Table $database.$table does not exist")
      return
    }

    println("\nTable schema:")
    spark.sql(s"DESCRIBE $database.$table").show(truncate = false)

    println("\nExtended table info:")
    spark.sql(s"DESCRIBE EXTENDED $database.$table").show(100, truncate = false)

    // Show sample data
    val count = spark.sql(s"SELECT COUNT(*) FROM $database.$table").collect()(0).getLong(0)
    println(s"\nRow count: $count")

    if (count > 0) {
      println("\nSample data:")
      spark.sql(s"SELECT * FROM $database.$table LIMIT 5").show(truncate = false)
    }
  }

  // ==========================
  // DROP Table Operation
  // ==========================
  private def dropTable(spark: SparkSession, database: String, table: String): Unit = {
    println(s"\n=== Dropping table: $database.$table ===")

    spark.sql(s"USE $database")

    // Check if table exists
    val tables = spark.sql(s"SHOW TABLES LIKE '$table'")
    if (tables.count() == 0) {
      println(s"Table $database.$table does not exist")
      return
    }

    // Get count before dropping
    val count = spark.sql(s"SELECT COUNT(*) FROM $database.$table").collect()(0).getLong(0)
    println(s"Table has $count rows")

    // Drop table
    spark.sql(s"DROP TABLE IF EXISTS $database.$table")
    println(s"Table $database.$table dropped successfully")

    // Verify
    val tablesAfter = spark.sql(s"SHOW TABLES LIKE '$table'")
    if (tablesAfter.count() == 0) {
      println("Verified: table no longer exists")
    }
  }

  // ==========================
  // Helper Functions
  // ==========================
  private def stripLocalPrefix(path: String): String = {
    if (path.startsWith("local://")) {
      path.stripPrefix("local://")
    } else if (path.startsWith("file://")) {
      path.stripPrefix("file://")
    } else {
      path
    }
  }

  private def getEnvWithFallback(primaryName: String, fallbackName: String, defaultValue: String): String = {
    sys.env.get(primaryName)
      .orElse(sys.env.get(fallbackName))
      .filter(_.nonEmpty)
      .getOrElse(defaultValue)
  }
}

