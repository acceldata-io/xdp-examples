import org.apache.spark.sql.{SparkSession, functions => F}

object Job1S3Read {
  def main(args: Array[String]): Unit = {

    // ==========================
    // Environment Variables (Driver)
    // ==========================
    val accessKey = sys.env.getOrElse("DATASTORE_AWS_ACCESS_KEY_ID","Your Access Key")
    val secretKey = sys.env.getOrElse("DATASTORE_AWS_SECRET_ACCESS_KEY","Your Secret Key")
    val bucketName = sys.env.getOrElse("DATASTORE_S3_BUCKET_NAME","Your Bucket Name")
    val s3FilePath = sys.env.getOrElse("DATASTORE_S3_FILE_PATH","Your File Path")
    val s3Region = sys.env.getOrElse("DATASTORE_S3_REGION","Your Region")
    


    // ==========================
    // Initialize Spark Session
    // ==========================
    val spark = SparkSession.builder()
      .appName("S3ReadApplication")
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

    spark.stop()
  }
}
