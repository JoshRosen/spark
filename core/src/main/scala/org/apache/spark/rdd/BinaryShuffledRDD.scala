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

private[spark] abstract class BinaryShuffledRDD[ParentType: ClassTag, OutputType: ClassTag](
    @transient var prev: RDD[ParentType],
    numPartitions: Int
  ) extends RDD[OutputType](prev.context, Nil) {

  // -- Methods to be overridden by subclasses ----------------------------------------------------

  def write(context: TaskContext, writer: BinaryShuffleWriter, iter: Iterator[ParentType]): Unit

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
    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(dep.shuffleId, split.index),
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024)
    read(context, blockFetcherItr.map(_._2))
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
