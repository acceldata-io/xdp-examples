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

  object VastDBOps {

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

      val spark = SparkSession.builder()
        .appName("Vast S3 Env Test Job")
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
      //show databases
      spark.sql("show databases").show(false)

      println("Successfully shown databases")
      spark.stop()
    }
  }