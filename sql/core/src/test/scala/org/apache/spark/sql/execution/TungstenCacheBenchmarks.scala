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

package org.apache.spark.sql.execution

import java.io.File

import scala.io.Source

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{SQLConf, Row, RandomDataGenerator, SQLContext, DataFrame}
import org.apache.spark.util.Utils

/**
 * Script for benchmarking the Tungsten Cache implementation for Spark SQL.
 */
object TungstenCacheBenchmarks {

  private implicit val format = DefaultFormats

  case class BenchmarkResult(cachedDataSizeBytes: Long, scanTimeMilliseconds: Long)

  /**
   * Executes the given function with a fresh SparkContext and SQLContext.
   */
  private def withFreshContext[T](fn: SQLContext => T): T = {
    val conf = new SparkConf().set("spark.ui.port", "0").set("spark.app.id", "dummyId")
    val sc = new SparkContext("local[4]", "TungstenCacheBenchmarks", conf)
    try {
      fn(new SQLContext(sc))
    } finally {
      sc.stop()
    }
  }

  /**
   * Executes the given block and reports the time taken (in milliseconds).
   */
  private def timeInMillis[T](block: => T): Long = {
    val startTime = System.currentTimeMillis()
    block
    val endTime = System.currentTimeMillis()
    endTime - startTime
  }

  /**
   * Uses the Spark UI's REST API to retrive the size, in bytes, of the cached RDD.
   * Assumes and asserts that only one RDD is cached and that all partitions are fully cached.
   */
  private def getCachedSizeInBytes(sc: SparkContext): Long = {
    val appUIEndpoint = sc.ui.get.appUIAddress
    val appName = sc.appName
    val storageEndpoint = s"$appUIEndpoint/api/v1/applications/$appName/storage/rdd"
    val json = Source.fromURL(storageEndpoint).getLines().mkString("\n")
    val parsed = JsonMethods.parse(json).asInstanceOf[JArray].arr
    assert(parsed.length == 1)
    assert(
      (parsed.head \ "numPartitions").extract[Int] ==
        (parsed.head \ "numCachedPartitions").extract[Int])
    (parsed.head \ "memoryUsed").extract[Long]
  }

  private def benchmarkFullScan(cachedDataFrame: DataFrame): BenchmarkResult = {
    // Force the data to be cached (necessary because caching is lazy):
    cachedDataFrame.rdd.count()
    // Figure out the size of the cached data:
    val cachedDataSizeBytes = getCachedSizeInBytes(cachedDataFrame.sqlContext.sparkContext)
    val durationMillis = timeInMillis {
      cachedDataFrame.queryExecution.sparkPlan.execute().count()
    }
    BenchmarkResult(
      cachedDataSizeBytes = cachedDataSizeBytes, scanTimeMilliseconds = durationMillis)
  }

  private def benchmarkPrunedScan(
      cachedDataFrame: DataFrame,
      cols: Seq[String]): BenchmarkResult = {
    // Force the data to be cached (necessary because caching is lazy):
    cachedDataFrame.rdd.count()
    // Figure out the size of the cached data:
    val cachedDataSizeBytes = getCachedSizeInBytes(cachedDataFrame.sqlContext.sparkContext)
    val durationMillis = timeInMillis {
      cachedDataFrame.select(cols.head, cols.tail: _*).queryExecution.sparkPlan.execute().count()
    }
    BenchmarkResult(
      cachedDataSizeBytes = cachedDataSizeBytes, scanTimeMilliseconds = durationMillis)
  }

  private def generateRandomDataFrame(
      sqlContext: SQLContext,
      schema: StructType,
      numRows: Long,
      numPartitions: Int): DataFrame = {
    val rows: RDD[Row] = sqlContext.sparkContext
      .parallelize(1L to numRows, numPartitions)
      .mapPartitions { iter =>
      val dataGenerator =
        RandomDataGenerator.forType(schema, nullable = false).get.asInstanceOf[() => Row]
      iter.map(_ => dataGenerator())
    }
    sqlContext.createDataFrame(rows, schema)
  }

  private def runBenchmark(
      numRepetitions: Int,
      testDataGenerator: SQLContext => DataFrame,
      cols: Seq[String] = Seq.empty // empty seq is treated as full scan
  ): Unit = {
    val tempDir = Utils.createTempDir()
    val dataPath = new File(tempDir, "inputData").getAbsolutePath

    withFreshContext { sqlContext =>
      // Save the input data into Parquet. We'll re-use this data across benchmarking runs.
      val inputData = testDataGenerator(sqlContext)
      inputData.write.parquet(dataPath)
      val numRows = inputData.count()
      val numPartitions = inputData.rdd.partitions.length
      val schema = inputData.schema

      println()
      println("=" * 80)
      println(s"Benchmark configuration:")
      println(s"Number of rows: $numRows")
      println(s"Number of partitions: $numPartitions")
      println(s"Columns to scan: " +
        (if (cols.isEmpty) schema.fieldNames.toSeq else cols).mkString(", "))
      println("Schema:")
      schema.printTreeString()
      println("=" * 80)
      println()
    }

    // First, run the benchmark with the existing columnar cache
    def testColumnarCache(compress: Boolean): Unit = {
      for (repetition <- 1 to numRepetitions) {
        System.gc()
        withFreshContext { sqlContext =>
          sqlContext.setConf(SQLConf.COMPRESS_CACHED.key, compress.toString)
          val inputData = sqlContext.read.parquet(dataPath)
          val cachedDataFrame = inputData.cache()
          val result = if (cols.isEmpty) {
            benchmarkFullScan(cachedDataFrame)
          } else {
            benchmarkPrunedScan(cachedDataFrame, cols)
          }
          println(s"Size: ${result.cachedDataSizeBytes}, scanTime: ${result.scanTimeMilliseconds}")
        }
      }
    }

    println("-" * 80)
    println("Columnar cache (uncompressed)")
    println("-" * 80)
    testColumnarCache(compress = false)

    println()
    println("-" * 80)
    println("Columnar cache (compressed)")
    println("-" * 80)
    testColumnarCache(compress = true)

    // Next, try running the same benchmark with different configurations of the Tungsten cache
    for (
      compressionType <- Seq("", "lzf", "lz4", "snappy");
      blockSize <- Seq(4 * 1000 * 1000)
    ) {
      println()
      println("-" * 80)
      println(s"Tungsten cache (compression = '$compressionType', blockSize = $blockSize)")
      println("-" * 80)
      for (repetition <- 1 to numRepetitions) {
        System.gc()
        withFreshContext { sqlContext =>
          val inputData = sqlContext.read.parquet(dataPath)
          val ctx = inputData.sqlContext
          import ctx.implicits._
          val cachedDataFrame = inputData.tungstenCache(compressionType, blockSize)
          val result = if (cols.isEmpty) {
            benchmarkFullScan(cachedDataFrame)
          } else {
            benchmarkPrunedScan(cachedDataFrame, cols)
          }
          println(s"Size: ${result.cachedDataSizeBytes}, scanTime: ${result.scanTimeMilliseconds}")
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val schema = new StructType((1 to 10).map(i => StructField(s"_$i", LongType)).toArray)
    runBenchmark(
      numRepetitions = 5,
      generateRandomDataFrame(_: SQLContext, schema, numRows = 1000 * 1000, numPartitions = 10),
      cols = Seq("_1", "_2", "_3"))
  }
}
