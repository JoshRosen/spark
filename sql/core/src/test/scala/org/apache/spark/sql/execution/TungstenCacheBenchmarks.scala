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
import org.apache.spark.sql.types.{LongType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, RandomDataGenerator, SQLContext, DataFrame}
import org.apache.spark.util.Utils

/**
 * Script for benchmarking the Tungsten Cache implementation for Spark SQL.
 */
object TungstenCacheBenchmarks {

  private implicit val format = DefaultFormats

  case class BenchmarkResult(cachedDataSizeBytes: Long, scanTimeMilliseconds: Long)

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

  /**
   * Given a cached DataFrame, run a benchmark of scan performance.
   */
  private def runScanBenchmark(cachedDataFrame: DataFrame): BenchmarkResult = {
    cachedDataFrame.rdd.count()
    // Figure out the size of the cached data:
    val cachedDataSizeBytes = getCachedSizeInBytes(cachedDataFrame.sqlContext.sparkContext)
    // Time a full table scan:
    val startTime = System.currentTimeMillis()
    cachedDataFrame.rdd.map(identity).count()
    val endTime = System.currentTimeMillis()
    val durationMillis = endTime - startTime
    BenchmarkResult(
      cachedDataSizeBytes = cachedDataSizeBytes,
      scanTimeMilliseconds = durationMillis)
  }

  private def withFreshContext[T](fn: SQLContext => T): T = {
    val conf = new SparkConf().set("spark.ui.port", "0").set("spark.app.id", "dummyId")
    val sc = new SparkContext("local[4]", "TungstenCacheBenchmarks", conf)
    try {
      fn(new SQLContext(sc))
    } finally {
      sc.stop()
    }
  }

  def main(args: Array[String]): Unit = {
    val NUM_ROWS = 10000 * 1000
    val NUM_PARTITIONS = 10
    val NUM_REPETITIONS = 5

    val tempDir = Utils.createTempDir()
    val dataPath = new File(tempDir, "dataFile").getAbsolutePath

    // Save the input data into Parquet. We'll re-use this data across benchmarking runs.
    withFreshContext { sqlContext =>
      val schema = new StructType(Array(
        StructField("name", LongType),
        StructField("age", IntegerType)
      ))
      val rows: RDD[Row] = sqlContext.sparkContext
        .parallelize(1 to NUM_ROWS, NUM_PARTITIONS)
        .mapPartitions { iter =>
          val dataGenerator =
            RandomDataGenerator.forType(schema, nullable = false).get.asInstanceOf[() => Row]
          iter.map(_ => dataGenerator())
        }
      val df = sqlContext.createDataFrame(rows, schema)
      df.write.parquet(dataPath)
    }

    // First, run the benchmark with the existing columnar cache
    println("-" * 80)
    println("Columnar cache")
    println("-" * 80)
    for (repetition <- 1 to NUM_REPETITIONS) {
      System.gc()
      withFreshContext { sqlContext =>
        val inputData = sqlContext.read.parquet(dataPath)
        val cachedDataFrame = inputData.cache()
        val result = runScanBenchmark(cachedDataFrame)
        println(s"Size: ${result.cachedDataSizeBytes}, scanTime: ${result.scanTimeMilliseconds}")
      }
    }

    // Next, try running the same benchmark with different configurations of the Tungsten cache
    for (
      compressionType <- Seq("", "lzf", "lz4", "snappy");
      blockSize <- Seq(4 * 1000 * 1000)
    ) {
      println()
      println("-" * 80)
      println(s"Tungsten cache (compression = '$compressionType', blockSize = $blockSize)")
      println("-" * 80)
      for (repetition <- 1 to NUM_REPETITIONS) {
        System.gc()
        withFreshContext { sqlContext =>
          val inputData = sqlContext.read.parquet(dataPath)
          val ctx = inputData.sqlContext
          import ctx.implicits._
          val cachedDataFrame = inputData.tungstenCache(compressionType, blockSize)
          val result = runScanBenchmark(cachedDataFrame)
          println(s"Size: ${result.cachedDataSizeBytes}, scanTime: ${result.scanTimeMilliseconds}")
        }
      }
    }
  }
}
