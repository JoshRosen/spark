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

private[spark] class BinaryShuffleWriter(
    indexShuffleBlockResolver: IndexShuffleBlockResolver,
    shuffleId: Int,
    mapId: Int) {

  private var aborted: Boolean = false
  private var _partitionLengths: Array[Long] = _

  def getDataFile(): File = {
    indexShuffleBlockResolver.getDataFile(shuffleId, mapId)
  }

  def commit(partitionLengths: Array[Long]): Unit = {
    require(_partitionLengths == null)
    indexShuffleBlockResolver.writeIndexFile(shuffleId, mapId, partitionLengths)
    _partitionLengths = partitionLengths
  }

  def getPartitionLengths(): Array[Long] = {
    require(_partitionLengths != null)
    _partitionLengths
  }

  def abort(): Unit = {
    require(_partitionLengths == null)
    if (!aborted) {
      aborted = true
      indexShuffleBlockResolver.removeDataByMap(shuffleId, mapId)
    }
  }
}
