package com.acceldata.spark.vast

import org.apache.spark.sql.SparkSession
import java.io.File

object LocalDirCleanupJob {

  // Milliseconds per day
  private val MS_PER_DAY = 24L * 60L * 60L * 1000L

  def main(args: Array[String]): Unit = {
    // Read paths and age threshold from command-line arguments
    if (args.length < 2) {
      throw new RuntimeException(
        """Usage: LocalDirCleanupJob <path1,path2,...> <days>
          |
          |Arguments:
          |  paths  - Comma-separated list of directories to clean
          |  days   - Only delete items not modified for this many days (0 = delete all)
          |
          |Examples:
          |  LocalDirCleanupJob /data0/spark-tmp,/data1/spark-tmp 1
          |  LocalDirCleanupJob /data0/spark-tmp 0
          |""".stripMargin)
    }
    val targetDirs = args(0).split(",").map(_.trim).filter(_.nonEmpty)
    val daysThreshold = args(1).toInt

    val spark = SparkSession.builder()
      .appName("SparkLocalDirCleanupJob")
      .getOrCreate()

    // Calculate cutoff time
    val currentTime = System.currentTimeMillis()
    val cutoffTime = currentTime - (daysThreshold * MS_PER_DAY)
    val cutoffDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(cutoffTime))

    println(s"=== Cleanup Configuration ===")
    println(s"Target directories: ${targetDirs.mkString(", ")}")
    println(s"Age threshold: $daysThreshold days")
    println(s"Will delete items not modified since: $cutoffDate")
    println(s"Current time: ${new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(currentTime))}")

    // Get number of executors from spark conf (more reliable than statusTracker)
    val numExecutors = spark.conf.get("spark.executor.instances", "1").toInt
    // Use more partitions to increase chance of hitting all nodes
    val numPartitions = numExecutors * 2
    println(s"Launching cleanup targeting $numExecutors executors with $numPartitions partitions...")

    // Create RDD with more partitions than executors to ensure coverage
    val rdd = spark.sparkContext.parallelize(1 to numPartitions, numPartitions)

    // Collect results from all partitions: (hostname, deleteCount, deleteSize, keepCount, keepSize, output)
    val allResults = rdd.mapPartitions { _ =>
      val hostname = java.net.InetAddress.getLocalHost.getHostName
      val output = new StringBuilder()
      output.append(s"=== Executor on node: $hostname ===\n")

      var totalDeleteCount = 0
      var totalDeleteSize = 0L
      var totalKeepCount = 0
      var totalKeepSize = 0L

      targetDirs.foreach { path =>
        val dir = new File(path)
        if (dir.exists() && dir.isDirectory) {
          val items = dir.listFiles()
          if (items != null) {
            // Check ONLY the directory's own modification time (fast - no file traversal)
            val (oldItems, recentItems) = items.partition { f =>
              f.lastModified() < cutoffTime
            }

            // Calculate sizes before deletion
            val deleteSize = oldItems.map(f => getDirSize(f)).sum
            val keepSize = recentItems.map(f => getDirSize(f)).sum

            // Delete old items
            oldItems.foreach(deleteRecursively)

            totalDeleteCount += oldItems.length
            totalDeleteSize += deleteSize
            totalKeepCount += recentItems.length
            totalKeepSize += keepSize

            output.append(s"${dir.getAbsolutePath}:\n")
            output.append(s"  Total items: ${items.length}\n")
            output.append(s"  DELETED: ${oldItems.length} items (not modified for $daysThreshold+ days) - ${humanReadableByteCountSI(deleteSize)}\n")
            output.append(s"  KEPT: ${recentItems.length} items (recently modified) - ${humanReadableByteCountSI(keepSize)}\n")
          } else {
            output.append(s"${dir.getAbsolutePath}: empty or not accessible\n")
          }
        } else {
          output.append(s"$path not found on $hostname\n")
        }
      }
      Iterator((hostname, totalDeleteCount, totalDeleteSize, totalKeepCount, totalKeepSize, output.toString()))
    }.collect()

    // Deduplicate by hostname - only show each node once
    val uniqueResults = allResults.groupBy(_._1).map { case (hostname, results) =>
      results.head
    }.toSeq

    // Print per-node results
    println(s"\nResults from ${uniqueResults.size} unique nodes:")
    println("=" * 60)
    uniqueResults.foreach { case (_, _, _, _, _, output) =>
      println(output)
    }

    // Calculate and print totals across all nodes
    val grandTotalDeleteCount = uniqueResults.map(_._2).sum
    val grandTotalDeleteSize = uniqueResults.map(_._3).sum
    val grandTotalKeepCount = uniqueResults.map(_._4).sum
    val grandTotalKeepSize = uniqueResults.map(_._5).sum

    println("=" * 60)
    println("=== CLEANUP SUMMARY ACROSS ALL NODES ===")
    println("=" * 60)
    println(s"Total nodes processed: ${uniqueResults.size}")
    println(s"Total DELETED: $grandTotalDeleteCount items - ${humanReadableByteCountSI(grandTotalDeleteSize)}")
    println(s"Total KEPT: $grandTotalKeepCount items - ${humanReadableByteCountSI(grandTotalKeepSize)}")
    println("=" * 60)

    spark.stop()
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) files.foreach(deleteRecursively)
    }
    file.delete()
  }

  def getDirSize(file: File): Long = {
    if (!file.exists()) 0L
    else if (file.isFile) file.length()
    else {
      val files = file.listFiles()
      if (files == null) 0L
      else files.map(getDirSize).sum
    }
  }

  def humanReadableByteCountSI(bytes: Long): String = {
    if (bytes < 1000) s"${bytes} B"
    else {
      val exp = (Math.log(bytes) / Math.log(1000)).toInt
      val pre = "kMGTPE".charAt(exp - 1)
      f"${bytes / Math.pow(1000, exp)}%.1f ${pre}B"
    }
  }
}