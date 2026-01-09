package com.acceldata.spark.vast

import org.apache.spark.sql.SparkSession

object CreateDirectoryInS3 {

  def main(args: Array[String]): Unit = {

    // Read path from command-line argument
    if (args.length < 1) {
      throw new RuntimeException("Usage: CreateDirectoryInS3 <path>")
    }
    val path = args(0)

    // Read from environment variables
    val accessKey   = sys.env.getOrElse("DATASTORE_AWS_ACCESS_KEY_ID", "")
    val secretKey   = sys.env.getOrElse("DATASTORE_AWS_SECRET_ACCESS_KEY", "")
    val authType    = sys.env.getOrElse("DATASTORE_S3_AUTHENTICATION_TYPE", "access-key")
    val bucket      = sys.env.getOrElse("DATASTORE_S3_BUCKET_NAME", "")
    val endpoint    = sys.env.getOrElse("DATASTORE_S3_ENDPOINT_URL", "")
    val region      = sys.env.getOrElse("DATASTORE_S3_REGION", "us-east-1")

    if (accessKey.isEmpty || secretKey.isEmpty || bucket.isEmpty || endpoint.isEmpty) {
      throw new RuntimeException("Missing required environment variables for S3 access.")
    }

    val spark = SparkSession.builder()
      .appName("CreateBucket")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.access.key", accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", secretKey)
      .config("spark.hadoop.fs.s3a.endpoint", endpoint)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .getOrCreate()

    // GPU validation
    val gpuExecutors = spark.conf.getOption("spark.executor.resource.gpu.amount").getOrElse("0").toInt
    val gpuTasks     = spark.conf.getOption("spark.task.resource.gpu.amount").getOrElse("0").toInt

    if (gpuExecutors > 0 || gpuTasks > 0) {
      println(s"GPU resources detected: executor=$gpuExecutors, task=$gpuTasks")
    } else {
      println("No GPU resources detected, running on CPU nodes.")
    }

    // Create sample DataFrame
    import spark.implicits._
    val df = Seq(
      (1, "alpha"),
      (2, "beta"),
      (3, "gamma")
    ).toDF("id", "value")

    val testPath = s"s3a://$bucket/$path"

    // Write
    df.write.mode("overwrite").parquet(testPath)
    println(s"Data written to $testPath")

    // Read back
    val readDf = spark.read.parquet(testPath)
    readDf.show(false)

    println("Successfully wrote and read back data from VAST S3 (via env vars)")
    spark.stop()
  }
}