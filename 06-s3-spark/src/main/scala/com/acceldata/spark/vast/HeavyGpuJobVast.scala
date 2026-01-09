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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HeavyGpuJobVast {
  def main(args: Array[String]): Unit = {
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

    val spark = SparkSession.builder
      .appName("HeavyGpuJobVast")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.access.key", accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", secretKey)
      .config("spark.hadoop.fs.s3a.endpoint", endpoint)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .getOrCreate()

    val sc = spark.sparkContext
    println("==== Spark Configuration Dump ====")
    sc.getConf.getAll.foreach { case (k, v) =>
      println(s"$k = $v")
    }

    import spark.implicits._
    val path = s"s3a://$bucket/heavygpujob/"

    // Generate synthetic data
    val bigDf = spark.range(0, 200000000)
      .withColumn("value1", (rand() * 10000).cast("double"))
      .withColumn("value2", (rand() * 5000).cast("double"))
      .withColumn("category", (rand() * 1000).cast("int"))

    println("=== Writing dataset to VAST (S3) ===")
    bigDf.write.mode("overwrite").parquet(path + "input")

    println("=== Reading dataset back from VAST (S3) ===")
    val vastDf = spark.read.parquet(path + "input")

    // Shuffle-heavy transformations
    val enrichedDf = vastDf
      .withColumn("value_ratio", col("value1") / (col("value2") + lit(1)))
      .repartition(200)

    val joinedDf = enrichedDf.alias("a")
      .join(enrichedDf.alias("b"), $"a.category" === $"b.category")
      .groupBy("a.category")
      .agg(
        avg($"a.value_ratio").as("avg_ratio"),
        sum($"b.value2").as("total_value2"),
        countDistinct($"a.value1").as("distinct_values")
      )

    println("=== Query Plan ===")
    joinedDf.explain(true)

    val start = System.currentTimeMillis()
    val result = joinedDf.collect()
    val end = System.currentTimeMillis()

    println(s"=== Job Finished. Total rows: ${result.length} ===")
    println(s"=== Execution Time: ${(end - start) / 1000.0} seconds ===")
    result.take(20).foreach(println)

    spark.stop()
  }
}