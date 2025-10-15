import org.apache.spark.sql.{SparkSession, functions => F}

object Job2S3Write {
  def main(args: Array[String]): Unit = {

    // ==========================
    // Environment Variables (Driver)
    // ==========================
    val accessKey = sys.env.getOrElse("DATASTORE_AWS_ACCESS_KEY_ID", "")
    val secretKey = sys.env.getOrElse("DATASTORE_AWS_SECRET_ACCESS_KEY", "")
    val bucketName = sys.env.getOrElse("DATASTORE_S3_BUCKET_NAME", "")
    val s3FilePath = sys.env.getOrElse("DATASTORE_S3_FILE_PATH", "")
    val s3FilePathOutput = sys.env.getOrElse("DATASTORE_S3_FILE_PATH_OUTPUT", "")
    val s3Region = sys.env.getOrElse("DATASTORE_S3_REGION", "")
    
    
    // ==========================
    // Initialize Spark Session
    // ==========================
    val spark = SparkSession.builder()
      .appName("S3WriteApplication")
      .getOrCreate()

    // ==========================
    // Configure S3 credentials in Hadoop configuration
    // ==========================
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.access.key", accessKey)
    hadoopConf.set("fs.s3a.secret.key", secretKey)
    hadoopConf.set("fs.s3a.endpoint.region", s3Region)
    
    // Optional: specify S3 endpoint if using non-AWS S3
    // hadoopConf.set("fs.s3a.endpoint", "s3.amazonaws.com")

    // ==========================
    // Build S3 File Path
    // ==========================
    val filePath = s"s3a://$bucketName/$s3FilePath"

    // ==========================
    // Read Data from S3
    // ==========================
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // ==========================
    // Show the data
    // ==========================
    df.show(truncate = false)

    // ==========================
    // Build Output File Path
    // ==========================
    val outPutFilePath = s"s3a://$bucketName/$s3FilePathOutput"

    // ==========================
    // Write Data to S3
    // ==========================
    df.write
      .option("header", "true")
      .mode("overwrite")
      .csv(outPutFilePath)

    // ==========================
    // Read Data from S3
    // ==========================
    val df_output = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(outPutFilePath)

    // ==========================
    // Show the data
    // ==========================
    df_output.show(truncate = false)

    spark.stop()
  }
}
