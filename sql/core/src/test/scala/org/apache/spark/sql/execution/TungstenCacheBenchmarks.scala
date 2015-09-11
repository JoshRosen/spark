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

  private def runBenchmark(
      inputData: DataFrame,
      compressionType: String,
      blockSize: Int): BenchmarkResult = {
    val ctx = inputData.sqlContext
    import ctx.implicits._
    val cachedData = inputData.tungstenCache(compressionType, blockSize)
    // Caching is lazy, so force execution in order to trigger caching:
    cachedData.rdd.count()
    // Figure out the size of the cached data:
    val cachedDataSizeBytes: Long = {
      val sc = inputData.sqlContext.sparkContext
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
    // Time a full table scan:
    val startTime = System.currentTimeMillis()
    cachedData.rdd.map(identity).count()
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

    for (
      repetition <- 1 to NUM_REPETITIONS;
      compressionType <- Seq("", "lzf", "lz4", "snappy");
      blockSize <- Seq(4 * 1000 * 1000)
    ) {
      System.gc()
      withFreshContext { sqlContext =>
        val inputData = sqlContext.read.parquet(dataPath)
        val result = runBenchmark(inputData, compressionType, blockSize)
        println(s"CompressionType: '$compressionType', blockSize: $blockSize")
        println(s"Size: ${result.cachedDataSizeBytes}, scanTime: ${result.scanTimeMilliseconds}")
      }
    }
  }
}
