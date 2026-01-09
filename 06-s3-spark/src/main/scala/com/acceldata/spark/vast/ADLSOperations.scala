package com.acceldata.spark.vast

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Azure Data Lake Storage Gen2 Operations using Apache Spark (Scala)
 *
 * This class supports read, write, and list operations on Azure Data Lake Storage Gen2
 * using service principal (OAuth2) authentication.
 *
 * Usage:
 *   ADLSOperations <operation> <path>
 *
 * Operations:
 *   read   <path>  - Read and display data from ADLS path
 *   write  <path>  - Write sample data to ADLS path
 *   list   <path>  - List files/directories at ADLS path
 *
 * Available Environment Variables:
 *   - DATASTORE_ADLS_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT
 *   - DATASTORE_ADLS_CONTAINER_NAME or AZURE_CONTAINER_NAME
 *
 * Author: Acceldata Platform Team
 * Company: acceldata.io
 */
object ADLSOperations {

  // ==========================
  // Environment Variables passed in xDP session
  // ==========================
  private lazy val storageAccount: String = getEnvWithFallback("DATASTORE_ADLS_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_ACCOUNT", "")
  private lazy val containerName: String = getEnvWithFallback("DATASTORE_ADLS_CONTAINER_NAME", "AZURE_CONTAINER_NAME", "")

  def main(args: Array[String]): Unit = {
    // ==========================
    // Parse Command-Line Arguments
    // ==========================
    if (args.length < 2) {
      throw new RuntimeException(
        """Usage: ADLSOperations <operation> <path>
          |
          |Operations:
          |  read   <path>  - Read and display data from ADLS path
          |  write  <path>  - Write sample data to ADLS path
          |  list   <path>  - List files/directories at ADLS path
          |
          |Examples:
          |  ADLSOperations read raw/events/sales.csv
          |  ADLSOperations write output/data
          |  ADLSOperations list raw/events
          |
          |Environment Variables Required:
          |  - DATASTORE_ADLS_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT
          |  - DATASTORE_ADLS_CONTAINER_NAME or AZURE_CONTAINER_NAME
          |""".stripMargin)
    }

    val operation = args(0).toLowerCase
    val adlsPath = args(1)

    // ==========================
    // Print Configuration
    // ==========================
    val separator = "=" * 60
    println(separator)
    println("Azure Data Lake Storage Operations - Acceldata")
    println(separator)
    println(s"Operation: $operation")
    println(s"Path: $adlsPath")
    println(s"Storage Account: $storageAccount")
    println(s"Container: $containerName")
    println(separator)

    // ==========================
    // Validate Configuration
    // ==========================
    validateConfiguration()

    // ==========================
    // Create Spark Session
    // ==========================
    val spark = createSparkSession()

    try {
      // Build full ABFS path
      val fullPath = buildAbfsPath(adlsPath)
      println(s"Full ABFS Path: $fullPath")

      operation match {
        case "read" =>
          readFromADLS(spark, fullPath)

        case "write" =>
          writeToADLS(spark, fullPath)

        case "list" =>
          listADLS(spark, fullPath)

        case _ =>
          throw new RuntimeException(s"Unknown operation: $operation. Supported: read, write, list")
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
  private def createSparkSession(): SparkSession = {
    println("Creating Spark session with Azure configuration...")

    val spark = SparkSession.builder()
      .appName("ADLSOperations - Acceldata")
      .getOrCreate()

    println("Spark session created successfully")
    spark
  }

  // ==========================
  // READ Operation
  // ==========================
  private def readFromADLS(spark: SparkSession, path: String): Unit = {
    println(s"\n=== Reading data from: $path ===")

    val format = detectFormat(path)
    println(s"Detected format: $format")

    val df = format match {
      case "csv" =>
        spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("multiLine", "true")
          .option("escape", "\"")
          .csv(path)

      case "parquet" =>
        spark.read.parquet(path)

      case "json" =>
        spark.read
          .option("multiline", "true")
          .json(path)

      case "orc" =>
        spark.read.orc(path)

      case _ =>
        // Try to read as text if format is unknown
        spark.read.text(path)
    }

    println("Schema:")
    df.printSchema()

    val rowCount = df.count()
    println(s"Row count: $rowCount")
    println(s"Column count: ${df.columns.length}")
    println(s"Columns: ${df.columns.mkString(", ")}")

    println("\nData preview (first 20 rows):")
    df.show(20, truncate = false)

    println(s"Successfully read data from $path")
  }

  // ==========================
  // WRITE Operation
  // ==========================
  private def writeToADLS(spark: SparkSession, path: String): Unit = {
    println(s"\n=== Writing sample data to: $path ===")

    import spark.implicits._

    // Generate sample data
    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("department", StringType, nullable = true),
      StructField("salary", DoubleType, nullable = true),
      StructField("region", StringType, nullable = true)
    ))

    val data = (1 to 100).map { i =>
      org.apache.spark.sql.Row(
        i,
        s"Employee_$i",
        s"Dept_${i % 5}",
        50000.0 + (i * 100),
        s"Region_${i % 10}"
      )
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    println("Sample data to write:")
    df.show(10, truncate = false)
    println(s"Total rows: ${df.count()}")

    val format = detectFormat(path)
    println(s"Writing as format: $format")

    format match {
      case "csv" =>
        df.write
          .option("header", "true")
          .mode("overwrite")
          .csv(path)

      case "parquet" =>
        df.write
          .option("compression", "snappy")
          .mode("overwrite")
          .parquet(path)

      case "json" =>
        df.write
          .mode("overwrite")
          .json(path)

      case "orc" =>
        df.write
          .mode("overwrite")
          .orc(path)

      case _ =>
        // Default to parquet
        df.write
          .option("compression", "snappy")
          .mode("overwrite")
          .parquet(path)
    }

    println(s"Successfully wrote data to $path")

    // Verify by reading back
    println("\nVerifying written data by reading back:")
    readFromADLS(spark, path)
  }

  // ==========================
  // LIST Operation
  // ==========================
  private def listADLS(spark: SparkSession, path: String): Unit = {
    println(s"\n=== Listing contents of: $path ===")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(new java.net.URI(path), hadoopConf)
    val hdfsPath = new Path(path)

    if (!fs.exists(hdfsPath)) {
      println(s"Path does not exist: $path")
      return
    }

    val status = fs.getFileStatus(hdfsPath)
    if (status.isFile) {
      println(s"$path is a file")
      printFileInfo(status)
    } else {
      println(s"$path is a directory\n")
      println(f"${"Type"}%-10s ${"Size"}%-15s ${"Modified"}%-25s ${"Name"}%s")
      println("-" * 80)

      val files = fs.listStatus(hdfsPath)
      var totalSize = 0L
      var fileCount = 0
      var dirCount = 0

      files.sortBy(_.getPath.getName).foreach { file =>
        val fileType = if (file.isDirectory) "DIR" else "FILE"
        val size = if (file.isDirectory) "-" else humanReadableByteCount(file.getLen)
        val modTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          .format(new java.util.Date(file.getModificationTime))
        val name = file.getPath.getName

        println(f"$fileType%-10s $size%-15s $modTime%-25s $name%s")

        if (file.isDirectory) dirCount += 1
        else {
          fileCount += 1
          totalSize += file.getLen
        }
      }

      println("-" * 80)
      println(s"Total: $dirCount directories, $fileCount files, ${humanReadableByteCount(totalSize)}")
    }
  }

  // ==========================
  // Helper Functions
  // ==========================
  private def buildAbfsPath(filePath: String): String = {
    // Remove leading slash if present
    val cleanPath = if (filePath.startsWith("/")) filePath.substring(1) else filePath
    s"abfss://$containerName@$storageAccount.dfs.core.windows.net/$cleanPath"
  }

  private def detectFormat(path: String): String = {
    val lowerPath = path.toLowerCase
    if (lowerPath.endsWith(".csv") || lowerPath.contains("/csv/") || lowerPath.endsWith("_csv")) "csv"
    else if (lowerPath.endsWith(".parquet") || lowerPath.contains("/parquet/") || lowerPath.endsWith("_parquet")) "parquet"
    else if (lowerPath.endsWith(".json") || lowerPath.contains("/json/") || lowerPath.endsWith("_json")) "json"
    else if (lowerPath.endsWith(".orc") || lowerPath.contains("/orc/") || lowerPath.endsWith("_orc")) "orc"
    else "parquet" // default
  }

  private def printFileInfo(status: org.apache.hadoop.fs.FileStatus): Unit = {
    println(f"${"Property"}%-20s ${"Value"}%s")
    println("-" * 60)
    println(f"${"Path"}%-20s ${status.getPath}%s")
    println(f"${"Size"}%-20s ${humanReadableByteCount(status.getLen)}%s")
    println(f"${"Block Size"}%-20s ${humanReadableByteCount(status.getBlockSize)}%s")
    println(f"${"Replication"}%-20s ${status.getReplication}%s")
    println(f"${"Owner"}%-20s ${status.getOwner}%s")
    println(f"${"Group"}%-20s ${status.getGroup}%s")
    println(f"${"Permission"}%-20s ${status.getPermission}%s")
    println(f"${"Modified"}%-20s ${new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(status.getModificationTime))}%s")
  }

  private def humanReadableByteCount(bytes: Long): String = {
    if (bytes < 1024) s"$bytes B"
    else {
      val exp = (Math.log(bytes) / Math.log(1024)).toInt
      val pre = "KMGTPE".charAt(exp - 1)
      f"${bytes / Math.pow(1024, exp)}%.1f ${pre}iB"
    }
  }

  private def maskString(str: String): String = {
    if (str.isEmpty) "<not set>"
    else if (str.length > 8) str.substring(0, 8) + "..."
    else str
  }

  private def validateConfiguration(): Unit = {
    println("Validating configuration...")

    val missing = Seq(
      ("DATASTORE_ADLS_STORAGE_ACCOUNT_NAME/AZURE_STORAGE_ACCOUNT", storageAccount),
      ("DATASTORE_ADLS_CONTAINER_NAME/AZURE_CONTAINER_NAME", containerName)
    ).filter(_._2.isEmpty).map(_._1)

    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"""Missing required environment variables:
           |${missing.map("  - " + _).mkString("\n")}
           |
           |Please set the following environment variables:
           |  XDP Format (preferred):
           |    - DATASTORE_ADLS_STORAGE_ACCOUNT_NAME
           |    - DATASTORE_ADLS_CONTAINER_NAME
           |  Or Standard Format:
           |    - AZURE_STORAGE_ACCOUNT
           |    - AZURE_CONTAINER_NAME
           |""".stripMargin)
    }

    println("Configuration validated successfully")
  }

  private def getEnvWithFallback(primaryName: String, fallbackName: String, defaultValue: String): String = {
    sys.env.get(primaryName)
      .orElse(sys.env.get(fallbackName))
      .filter(_.nonEmpty)
      .getOrElse(defaultValue)
  }
}


