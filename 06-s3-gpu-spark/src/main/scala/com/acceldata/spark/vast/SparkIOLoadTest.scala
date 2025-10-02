/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.acceldata.spark.vast

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object SparkIOLoadTest {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SparkIOLoadTest <testCase> <scaleFactor>")
      System.exit(1)
    }

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

    val testCase = args(0).toLowerCase
    val scaleFactor = args(1).toLong
    val outputPath = if (args.length > 2) args(2) else s"s3a://$bucket/tmp/spark_env_test/benchmark/"

    val spark = SparkSession.builder()
      .appName(s"I/O Benchmark - $testCase")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.access.key", accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", secretKey)
      .config("spark.hadoop.fs.s3a.endpoint", endpoint)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .getOrCreate()

    testCase match {
      case "write" =>
        runSequentialWrite(spark, scaleFactor, s"$outputPath/seq_write")

      case "read" =>
        runSequentialRead(spark, s"$outputPath/seq_write")

      case "shuffleagg" =>
        runShuffleAgg(spark, scaleFactor, s"$outputPath/shuffle_agg")

      case "mixedetl" =>
        runMixedETL(spark, scaleFactor, s"$outputPath/mixed_etl")

      case "randomread" =>
        runRandomRead(spark, s"$outputPath/seq_write")

      case _ =>
        println(s"Unknown testCase: $testCase")
    }

    spark.stop()
  }

  // Helper to add 25 columns
  def withWideColumns(df: DataFrame): DataFrame = {
    df
      .withColumn("col_str", concat(lit("val_"), col("key")))
      .withColumn("col_double", (col("key") * 1.5).cast("double"))
      .withColumn("col_bool", (col("key") % 2 === 0))
      .withColumn("col_date", date_add(lit("2020-01-01"), (col("key") % 1000).cast("int")))
      .withColumn("col_array", array(col("key"), col("key") + 1, col("key") + 2))
      .withColumn("col_struct", struct(col("key").as("id_val"), col("col_str").as("name")))
      .withColumn("col_int", (col("key") * 3).cast("int"))
      .withColumn("col_long", (col("key") * 1000L))
      .withColumn("col_float", (col("key") / 3.14).cast("float"))
      .withColumn("col_short", (col("key") % 100).cast("short"))
      .withColumn("col_byte", (col("key") % 10).cast("byte"))
      .withColumn("col_decimal", (col("key") * 1.2345).cast("decimal(18,4)"))
      .withColumn("col_ts", current_timestamp())
      .withColumn("col_map", map(lit("k1"), col("key"), lit("k2"), (col("key") + 100)))
      .withColumn("col_str_len", length(col("col_str")))
      .withColumn("col_hash", sha2(col("col_str"), 256))
      .withColumn("col_upper", upper(col("col_str")))
      .withColumn("col_lower", lower(col("col_str")))
      .withColumn("col_rand", rand())
      .withColumn("col_rand_int", (rand() * 1000).cast("int"))
      .withColumn("col_mod", col("key") % 7)
      .withColumn("col_bucket", (col("key") % 500).cast("int"))
      .withColumn("col_isnull", col("col_str").isNull)
      .withColumn("col_case", when(col("key") % 2 === 0, "even").otherwise("odd"))
  }

  // --- Case 1: Sequential Write ---
  def runSequentialWrite(spark: SparkSession, scale: Long, path: String): Unit = {
    val df = spark.range(0, scale).withColumnRenamed("id", "key")
    val wideDf = withWideColumns(df)
    wideDf.write.mode("overwrite").parquet(path)
    println(s"Wrote $scale rows with ${wideDf.columns.length} columns to $path")
  }

  // --- Case 2: Sequential Read ---
  def runSequentialRead(spark: SparkSession, path: String): Unit = {
    val df = spark.read.parquet(path)
    println(s"Row count = ${df.count()}, Columns = ${df.columns.length}")
  }

  // --- Case 3: Shuffle + Aggregation ---
  def runShuffleAgg(spark: SparkSession, scale: Long, path: String): Unit = {
    val df = spark.range(0, scale).withColumnRenamed("id", "key")
    val wideDf = withWideColumns(df)
    val agg = wideDf.groupBy("col_bucket").agg(count("*").as("cnt"), sum("key").as("sum_key"))
    agg.write.mode("overwrite").parquet(path)
    println(s"Wrote aggregated results to $path with ${agg.columns.length} columns")
  }

  // --- Case 4: Mixed ETL ---
  def runMixedETL(spark: SparkSession, scale: Long, path: String): Unit = {
    val df = spark.range(0, scale).withColumnRenamed("id", "key")
    val wideDf = withWideColumns(df)
    wideDf.repartition(200, col("col_bucket"))
      .write.mode("overwrite").parquet(path)
    println(s"Wrote transformed dataset to $path with ${wideDf.columns.length} columns")
  }

  // --- Case 5: Random Read ---
  def runRandomRead(spark: SparkSession, path: String): Unit = {
    val df = spark.read.parquet(path)
    val sample = df.filter("key % 100 == 0")
    println(s"Sample count = ${sample.count()}, Columns = ${df.columns.length}")
  }
}