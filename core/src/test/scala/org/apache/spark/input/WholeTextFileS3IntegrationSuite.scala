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

package org.apache.spark.input

import java.net.URI

import scala.util.Random

import org.apache.hadoop.fs.{Path, FileSystem}
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll}

import org.apache.spark.{SharedSparkContext, SparkFunSuite}

/**
 * This suite contains integration tests for reproducing a number of bugs involving
 * the [[org.apache.spark.SparkContext.wholeTextFiles()]] method and Amazon S3 (see SPARK-11176, an
 * umbrella ticket, for a complete list.
 *
 * These tests are configured to run against a real S3 bucket and thus are heavyweight to run.
 * As a result, they're disabled by default and do not run in Jenkins. These tests are checked into
 * the repository to make sure that we can find them if we ever need them again in the future.
 */
class WholeTextFileS3IntegrationSuite
  extends SparkFunSuite
  with SharedSparkContext
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  private var fs: FileSystem = _
  private var tempDir: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val AWS_ACCESS_KEY_ID: String = sys.env.get("TEST_AWS_ACCESS_KEY_ID").get
    val AWS_SECRET_ACCESS_KEY: String = sys.env.get("TEST_AWS_SECRET_ACCESS_KEY").get
    // Path to a directory in S3 (e.g. 's3n://bucket-name/path/to/scratch/space').
    val AWS_S3_SCRATCH_SPACE: String = sys.env.get("AWS_S3_SCRATCH_SPACE").get

    // Just to be on the safe-side, we append a random suffix to the user-provided scratch space
    // and only create files underneath that suffix. When it comes time to delete temporary files,
    // this ensures that we we won't delete all of a user's files in case they happened to point
    // AWS_S3_SCRATCH_SPACE to the root of a bucket which contains real data that should not be
    // deleted.
    tempDir = AWS_S3_SCRATCH_SPACE + Math.abs(Random.nextLong()).toString

    // Bypass Hadoop's FileSystem caching mechanism so that we don't cache the credentials:
    sc.hadoopConfiguration.setBoolean("fs.s3.impl.disable.cache", true)
    sc.hadoopConfiguration.setBoolean("fs.s3n.impl.disable.cache", true)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
    fs = FileSystem.get(URI.create(tempDir), sc.hadoopConfiguration)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    fs.delete(new Path(tempDir), true)
  }

  override def afterAll(): Unit = {
    try {
      if (fs != null) {
        fs.delete(new Path(tempDir), true)
        fs.close()
      }
    } finally {
      super.afterAll()
    }
  }

  test("Read file with zero bytes (SPARK-11177)") {
    // SPARK-11177 was only reproducible on Hadoop 1.x builds
    val emptyFilePath = new Path(tempDir, "emptyFile")
    assert(fs.createNewFile(emptyFilePath))
    assert(fs.getFileStatus(emptyFilePath).getLen === 0)
    assert(sc.wholeTextFiles(tempDir).keys.collect() === Array(emptyFilePath.toString))
    assert(sc.wholeTextFiles(tempDir).values.flatMap(_.split("\n")).count() === 1)
  }

  test("Read individual .txt file instead of directory of files (SPARK-4414)") {
    // SPARK-4414 was only reproducible on Hadoop 1.0.4 builds (and possibly earlier) but not
    // on Hadoop 1.2.1 or newer.
    val testFilePath = new Path(tempDir, "testFile.txt")
    val os = fs.create(testFilePath)
    os.write("Hello".getBytes("utf-8"))
    os.close()
    val pathToRead = testFilePath.toString
    assert(sc.wholeTextFiles(pathToRead).keys.collect() === Array(pathToRead))
    assert(sc.wholeTextFiles(pathToRead).values.flatMap(_.split("\n")).collect() === Array("Hello"))
  }
}
