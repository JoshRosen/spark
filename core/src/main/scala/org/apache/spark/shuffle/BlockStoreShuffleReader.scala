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

package org.apache.spark.shuffle

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
 */
private[spark] class BlockStoreShuffleReader[T](
    handle: BaseShuffleHandle[_, _, _],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[T] with Logging {

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[T] = {
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue))

    // Wrap the streams for compression based on configuration
    val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
      blockManager.wrapForCompression(blockId, inputStream)
    }

    val serializerInstance = dep.serializer.newInstance()

    // Create an iterator for each stream
    val recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asIterator.asInstanceOf[Iterator[T]]
    }

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.registerTempShuffleReadMetrics()
    val metricIter = CompletionIterator[T, Iterator[T]](
      recordIter.map(record => {
        readMetrics.incRecordsRead(1)
        record
      }),
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    new InterruptibleIterator[T](context, metricIter)
  }
}
