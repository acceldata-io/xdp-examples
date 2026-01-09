package com.acceldata.spark.vast

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSOperations {

  def main(args: Array[String]): Unit = {
    // ==========================
    // Parse Command-Line Arguments
    // ==========================
    if (args.length < 2) {
      throw new RuntimeException(
        """Usage: HDFSOperations <operation> <path> [options]
          |
          |Operations:
          |  read   <path>              - Read and display data from HDFS path
          |  write  <path>              - Write sample data to HDFS path
          |  list   <path>              - List files/directories at HDFS path
          |
          |Examples:
          |  HDFSOperations read /user/data/input.csv
          |  HDFSOperations write /user/data/output
          |  HDFSOperations list /user/data
          |""".stripMargin)
    }

    val operation = args(0).toLowerCase
    val hdfsPath = args(1)

    // ==========================
    // Environment Variables passed in xDP session
    // ==========================
    val kerberosKeytabRaw = sys.env.getOrElse("KERBEROS_KEYTAB", "")
    val hdfsUrl = sys.env.getOrElse("HDFS_URL", "")
    val kerberosPrincipal = sys.env.getOrElse("KERBEROS_PRINCIPAL", "")

    // Strip local:// prefix if present (K8s uses this format but UGI expects file path)
    val kerberosKeytab = stripLocalPrefix(kerberosKeytabRaw)

    println(s"Operation: $operation")
    println(s"HDFS Path: $hdfsPath")
    println(s"HDFS URL: $hdfsUrl")
    println(s"Kerberos Principal: $kerberosPrincipal")
    println(s"Kerberos Keytab (raw): $kerberosKeytabRaw")
    println(s"Kerberos Keytab (resolved): $kerberosKeytab")

    // ==========================
    // Initialize Spark Session
    // ==========================
    val spark = SparkSession.builder()
      .appName(s"HDFSOperations-${operation.capitalize}")
      .getOrCreate()

    // ==========================
    // Authenticate with Kerberos (if keytab provided)
    // ==========================
    if (kerberosPrincipal.nonEmpty && kerberosKeytab.nonEmpty) {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("hadoop.security.authentication", "kerberos")
      hadoopConf.set("hadoop.security.authorization", "true")
      UserGroupInformation.setConfiguration(hadoopConf)
      UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab)
      println("Kerberos authentication successful")
    } else {
      println("Skipping Kerberos authentication (no principal/keytab provided)")
    }

    // ==========================
    // Build Full HDFS Path
    // ==========================
    val fullPath = if (hdfsUrl.nonEmpty) s"$hdfsUrl$hdfsPath" else hdfsPath

    try {
      operation match {
        case "read" =>
          readFromHDFS(spark, fullPath)

        case "write" =>
          writeToHDFS(spark, fullPath)

        case "list" =>
          listHDFS(spark, fullPath)

        case _ =>
          throw new RuntimeException(s"Unknown operation: $operation. Supported: read, write, list")
      }
    } finally {
      spark.stop()
    }
  }

  // ==========================
  // READ Operation
  // ==========================
  def readFromHDFS(spark: SparkSession, path: String): Unit = {
    println(s"\n=== Reading data from: $path ===")

    val format = detectFormat(path)
    println(s"Detected format: $format")

    val df = format match {
      case "csv" =>
        spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(path)

      case "parquet" =>
        spark.read.parquet(path)

      case "json" =>
        spark.read.json(path)

      case "orc" =>
        spark.read.orc(path)

      case _ =>
        // Try to read as text if format is unknown
        spark.read.text(path)
    }

    println(s"Schema:")
    df.printSchema()

    println(s"Row count: ${df.count()}")
    println(s"Data preview:")
    df.show(20, truncate = false)

    println(s"Successfully read data from $path")
  }

  // ==========================
  // WRITE Operation
  // ==========================
  def writeToHDFS(spark: SparkSession, path: String): Unit = {
    println(s"\n=== Writing sample data to: $path ===")

    import spark.implicits._

    // Create sample DataFrame
    val df = Seq(
      (1, "Alice", "Engineering", 75000),
      (2, "Bob", "Marketing", 65000),
      (3, "Charlie", "Engineering", 80000),
      (4, "Diana", "Sales", 70000),
      (5, "Eve", "Marketing", 72000)
    ).toDF("id", "name", "department", "salary")

    println("Sample data to write:")
    df.show(truncate = false)

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
          .mode("overwrite")
          .parquet(path)
    }

    println(s"Successfully wrote data to $path")

    // Verify by reading back
    println("\nVerifying written data by reading back:")
    readFromHDFS(spark, path)
  }

  // ==========================
  // LIST Operation
  // ==========================
  def listHDFS(spark: SparkSession, path: String): Unit = {
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
  def detectFormat(path: String): String = {
    val lowerPath = path.toLowerCase
    if (lowerPath.endsWith(".csv") || lowerPath.contains("/csv/") || lowerPath.endsWith("_csv")) "csv"
    else if (lowerPath.endsWith(".parquet") || lowerPath.contains("/parquet/") || lowerPath.endsWith("_parquet")) "parquet"
    else if (lowerPath.endsWith(".json") || lowerPath.contains("/json/") || lowerPath.endsWith("_json")) "json"
    else if (lowerPath.endsWith(".orc") || lowerPath.contains("/orc/") || lowerPath.endsWith("_orc")) "orc"
    else "parquet" // default
  }

  def printFileInfo(status: org.apache.hadoop.fs.FileStatus): Unit = {
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

  def humanReadableByteCount(bytes: Long): String = {
    if (bytes < 1024) s"$bytes B"
    else {
      val exp = (Math.log(bytes) / Math.log(1024)).toInt
      val pre = "KMGTPE".charAt(exp - 1)
      f"${bytes / Math.pow(1024, exp)}%.1f ${pre}iB"
    }
  }

  /**
   * Strip local:// or file:// prefix from path
   * K8s Spark uses local:// format but UserGroupInformation expects plain file path
   */
  def stripLocalPrefix(path: String): String = {
    if (path.startsWith("local://")) {
      path.stripPrefix("local://")
    } else if (path.startsWith("file://")) {
      path.stripPrefix("file://")
    } else {
      path
    }
  }
}

