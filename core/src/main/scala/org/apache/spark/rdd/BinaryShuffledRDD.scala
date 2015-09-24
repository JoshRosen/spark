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

package org.apache.spark.rdd

import java.io.InputStream

import scala.reflect.ClassTag

import org.apache.spark.{BinaryShuffleDependency, Dependency, SparkEnv, TaskContext, Partition}
import org.apache.spark.shuffle.BinaryShuffleWriter
import org.apache.spark.storage.ShuffleBlockFetcherIterator

/**
 * This class allows non-core Spark components to perform shuffles with customized data writing
 * and reading. It effectively allows non-core components to customize all aspects of shuffle except
 * for the actual network transfer.
 *
 * Compared to the existing shuffle APIs, such as [[ShuffledRDD]] and
 * [[org.apache.spark.ShuffleDependency]], this API is lower-level and thus more general:
 *
 *   - The shuffle write and read paths are not record-oriented and are not coupled to Scala tuples
 *     or key-value pairs. This allows custom shuffle implementations to operate on batches of
 *     records or to write shuffle data in a column-oriented format.
 *   - This API says nothing about serialization, sorting, aggregation, or partitioning; instead,
 *     it only captures the structure of the wide dependency and exposes a low-level interface for
 *     writing opaque binary shuffle data.
 *   - The write interfaces only expose file names and success / abort methods. As a result, the
 *     actual shuffle writing may be performed by an external process or by native (non-JVM) code.
 *
 * Note that this is _not_ intended to be a DeveloperApi yet and thus should not be used by code
 * outside of Spark itself. While we may eventually stabilize a version of this API, the current
 * API is not subject to any binary compatibility guarantees and thus should not be used by
 * third-party code.
 *
 * @param prev the RDD being shuffled.
 * @param numPartitions the number of partitions that will result from the shuffle.
 * @tparam ParentType the type of records in the RDD being shuffled.
 * @tparam OutputType the type of records in the post-shuffle RDD. Note that this may be different
 *                    than [[ParentType]] if the custom shuffle is performing aggregation or
 *                    grouping.
 */
private[spark] abstract class BinaryShuffledRDD[ParentType: ClassTag, OutputType: ClassTag](
    @transient var prev: RDD[ParentType],
    numPartitions: Int
  ) extends RDD[OutputType](prev.context, Nil) {

  // -- Methods to be implemented by subclasses ---------------------------------------------------

  /**
   * Called on the map side to write shuffle data.
   *
   * Note that it is the implementor's responsibility to perform any desired compression of the
   * shuffle data.
   *
   * @param context the TaskContext.
   * @param writer a handle which exposes the path where shuffle data should be written.
   *               See the documentation in [[BinaryShuffleWriter]] for a description of the
   *               expected file format.
   * @param iter an iterator of records from the RDD being shuffled.
   */
  def write(context: TaskContext, writer: BinaryShuffleWriter, iter: Iterator[ParentType]): Unit

  /**
   * Called on the reduce side to read shuffle data.
   *
   * @param context the TaskContext.
   * @param iter an iterator of input streams, one per map partition. These streams correspond,
   *             verbatim, to the per-reducer data written on the map side.
   * @return an iterator of records that were decoded from the input streams.
   */
  def read(context: TaskContext, iter: Iterator[InputStream]): Iterator[OutputType]

  // -- Private internal methods ------------------------------------------------------------------

  final override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](numPartitions)(i => new ShuffledRDDPartition(i))
  }

  final override def getDependencies: Seq[Dependency[_]] = {
    List(new BinaryShuffleDependency[ParentType](prev, numPartitions, write))
  }

  final override def compute(split: Partition, context: TaskContext): Iterator[OutputType] = {
    val dep = dependencies.head.asInstanceOf[BinaryShuffleDependency[ParentType]]
    val blockManager = SparkEnv.get.blockManager
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(dep.shuffleId, split.index),
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024)
    read(context, blockFetcherItr.map(_._2))
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
