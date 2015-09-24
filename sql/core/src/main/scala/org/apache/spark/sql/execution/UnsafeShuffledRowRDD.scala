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

import java.io.InputStream

import scala.util.control.NonFatal

import org.apache.spark.shuffle.BinaryShuffleWriter
import org.apache.spark.shuffle.unsafe.UnsafeShuffleWriter
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.{InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.CompletionIterator
import org.apache.spark.rdd.{BinaryShuffledRDD, RDD}


class UnsafeShuffledRowRDD(
    prev: RDD[Product2[Int, UnsafeRow]],
    numFields: Int,
    numPartitions: Int)
  extends BinaryShuffledRDD[Product2[Int, UnsafeRow], UnsafeRow](prev, numPartitions) {

  private val ser = new UnsafeRowSerializer(numFields)

  override def write(
      context: TaskContext,
      binaryWriter: BinaryShuffleWriter,
      iter: Iterator[Product2[Int, UnsafeRow]]): Unit = {
    val env = SparkEnv.get
    val writer = new UnsafeShuffleWriter[Int, UnsafeRow](
      env.blockManager,
      binaryWriter,
      context.taskMemoryManager(),
      env.shuffleMemoryManager,
      new PartitionIdPassthrough(numPartitions),
      Some(ser),
      context,
      env.conf)
    try {
      writer.write(iter)
      writer.stop(true)
    } catch {
      case NonFatal(e) =>
        writer.stop(false)
    }
  }

  override def read(context: TaskContext, blockIter: Iterator[InputStream]): Iterator[UnsafeRow] = {
    val wrappedStreams = blockIter.map { inputStream =>
      // TODO: configure / respect compression settings
      CompressionCodec.createCodec(SparkEnv.get.conf).compressedInputStream(inputStream)
    }
    val serializerInstance = ser.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }.map(_._2).asInstanceOf[Iterator[UnsafeRow]]

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics().createShuffleReadMetricsForDependency()
    val metricIter = CompletionIterator[UnsafeRow, Iterator[UnsafeRow]](
      recordIter.map(record => {
        readMetrics.incRecordsRead(1)
        record
      }),
      context.taskMetrics().updateShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    new InterruptibleIterator[UnsafeRow](context, metricIter)
  }
}
