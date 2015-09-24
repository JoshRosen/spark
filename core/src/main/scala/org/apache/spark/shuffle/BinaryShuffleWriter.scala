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

import java.io.File

/**
 * A low-level interface for writing shuffle output. This interface only exposes the path of the
 * data file where data should be written, plus two methods for signalling the completion of a
 * shuffle write or for aborting a failed write.
 *
 * The purpose of this low-level interface is to allow shuffle write to be performed by native
 * (non-JVM) code or by external processes.
 */
private[spark] class BinaryShuffleWriter(
    indexShuffleBlockResolver: IndexShuffleBlockResolver,
    shuffleId: Int,
    mapId: Int) {

  private var aborted: Boolean = false
  private var _partitionLengths: Array[Long] = _

  /**
   * Returns the file where shuffle data should be written. Within this file, the output partitions'
   * data should be laid out contiguously in order of reduce partition id. In other words, the file
   * should be laid out like [reduce partition 0's data][reduce partition 1's data]...
   */
  lazy val getDataFile: File = {
    indexShuffleBlockResolver.getDataFile(shuffleId, mapId)
  }

  /**
   * Call this method after writing a data file in order to signal a successful shuffle write.
   *
   * @param partitionLengths an array specifying the lengths of each reduce partition's data. These
   *                         lengths will be used to serve regions of the shuffle output file to
   *                         reducers.
   */
  def commit(partitionLengths: Array[Long]): Unit = {
    require(_partitionLengths == null)
    indexShuffleBlockResolver.writeIndexFile(shuffleId, mapId, partitionLengths)
    _partitionLengths = partitionLengths
  }

  /**
   * Call this method from error-handling code to perform cleanup after an unsuccessful shuffle
   * write.
   */
  def abort(): Unit = {
    require(_partitionLengths == null)
    if (!aborted) {
      aborted = true
      indexShuffleBlockResolver.removeDataByMap(shuffleId, mapId)
    }
  }

  /**
   * Called by [[org.apache.spark.scheduler.ShuffleMapTask]] when constructing a MapStatus.
   */
  private[spark] def getPartitionLengths: Array[Long] = {
    require(_partitionLengths != null)
    _partitionLengths
  }
}
