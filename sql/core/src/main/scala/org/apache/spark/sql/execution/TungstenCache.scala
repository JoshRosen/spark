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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.google.common.io.ByteStreams

import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.{TaskMemoryManager, MemoryBlock}
import org.apache.spark.{TaskContext, SparkEnv}
import org.apache.spark.annotation.Experimental
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
 * ::Experimental::
 *
 * @since 1.6.0
 */
@Experimental
object TungstenCache {
  def cache(
      child: SparkPlan,
      compressionType: Option[String] = None,
      blockSize: Int = 4 * 1000 * 1000): RDD[InternalRow] = {
    val END_OF_BLOCK: Int = -1
    val numFields = child.schema.length
    val childRDD: RDD[UnsafeRow] = {
      if (child.outputsUnsafeRows) {
        child.execute().asInstanceOf[RDD[UnsafeRow]]
      } else {
        ConvertToUnsafe(child).execute().asInstanceOf[RDD[UnsafeRow]]
      }
    }
    val cachedRDD: RDD[MemoryBlock] = childRDD.mapPartitions { rowIterator =>
      // Note: this buffering is used to let us hold onto rows when rolling across block boundaries.
      val bufferedRowIterator = rowIterator.buffered
      val taskMemoryManager = TaskContext.get().taskMemoryManager()
      val compressionCodec: Option[CompressionCodec] =
        compressionType.map { t => CompressionCodec.createCodec(SparkEnv.get.conf, t) }
      new Iterator[MemoryBlock] {
        // NOTE: This assumes that size of every row < blockSize - 4
        // TODO(josh): we'll have to figure out how to support large rows.
        def next(): MemoryBlock = {
          // Packs rows into a `blockSize` bytes contiguous block of memory, starting a new block
          // whenever the current fills up.
          // Each row is laid out in memory as [rowSize (int)|rowData (rowSize bytes)].
          // The end of the block is marked by a special rowSize, END_OF_BLOCK (-1).
          val block = taskMemoryManager.allocateUnchecked(blockSize)

          var currOffset = 0
          while (bufferedRowIterator.hasNext &&
            currOffset + 4 + bufferedRowIterator.head.getSizeInBytes < blockSize - 4) {
            val currRow = bufferedRowIterator.head
            Platform.putInt(
              block.getBaseObject, block.getBaseOffset + currOffset, currRow.getSizeInBytes)
            currRow.writeToMemory(block.getBaseObject, block.getBaseOffset + currOffset + 4)
            bufferedRowIterator.next()
            currOffset += 4 + currRow.getSizeInBytes
          }
          Platform.putInt(block.getBaseObject, block.getBaseOffset + currOffset, END_OF_BLOCK)

          compressionCodec match {
            case Some(codec) => compressBlock(block, codec, TaskContext.get().taskMemoryManager())
            case None => block
          }
        }

        def hasNext: Boolean = bufferedRowIterator.hasNext
      }
    }
    .setName(compressionType.getOrElse("") + "_" + child.nodeName)
    .persist(StorageLevel.MEMORY_ONLY)
    // TODO(josh): is this the right name?

    cachedRDD.mapPartitions { blockIterator =>
      val compressionCodec: Option[CompressionCodec] =
        compressionType.map { t => CompressionCodec.createCodec(SparkEnv.get.conf, t) }
      blockIterator.flatMap { rawBlock =>
        val block: MemoryBlock = compressionCodec match {
          case Some(codec) => decompressBlock(rawBlock, blockSize, codec)
          case None => rawBlock
        }

        new Iterator[UnsafeRow] {
          private[this] val unsafeRow = new UnsafeRow()
          private[this] var currOffset: Long = 0
          private[this] var _nextRowSize = getNextRowSize()
          private[this] def getNextRowSize(): Int = {
            if (currOffset >= block.size()) {
              -1
            } else {
              Platform.getInt(block.getBaseObject, block.getBaseOffset + currOffset)
            }
          }
          override def hasNext: Boolean = _nextRowSize != END_OF_BLOCK
          override def next(): UnsafeRow = {
            assert(_nextRowSize > 0)
            currOffset += 4
            unsafeRow.pointTo(
              block.getBaseObject, block.getBaseOffset + currOffset, numFields, _nextRowSize)
            currOffset += _nextRowSize
            _nextRowSize = getNextRowSize()
            unsafeRow
          }
        }
      }
    }
  }

  private def compressBlock(
      memoryBlock: MemoryBlock,
      compressionCodec: CompressionCodec,
      taskMemoryManager: TaskMemoryManager): MemoryBlock = {
    // Compress the block using an on-heap byte array
    val compressedBlockArray: Array[Byte] = {
      val blockArray = new Array[Byte](memoryBlock.size().toInt)
      Platform.copyMemory(
        memoryBlock.getBaseObject,
        memoryBlock.getBaseOffset,
        blockArray,
        Platform.BYTE_ARRAY_OFFSET,
        memoryBlock.size())
      val baos = new ByteArrayOutputStream(memoryBlock.size().toInt)
      val compressedBaos = compressionCodec.compressedOutputStream(baos)
      compressedBaos.write(blockArray)
      compressedBaos.flush()
      compressedBaos.close()
      baos.toByteArray
    }

    // Allocate a new block with compressed byte array padded to word boundary
    val totalRecordSize = compressedBlockArray.length + 4 // data + int to store size of padding
    val nearestWordBoundary =
      ByteArrayMethods.roundNumberOfBytesToNearestWord(totalRecordSize)
    val padding = nearestWordBoundary - totalRecordSize
    val compressedBlock = taskMemoryManager.allocateUnchecked(totalRecordSize + padding)
    Platform.putInt(
      compressedBlock.getBaseObject,
      compressedBlock.getBaseOffset,
      padding)
    Platform.copyMemory(
      compressedBlockArray,
      Platform.BYTE_ARRAY_OFFSET,
      compressedBlock.getBaseObject,
      compressedBlock.getBaseOffset + 4,
      compressedBlockArray.length)
    taskMemoryManager.freeUnchecked(memoryBlock)
    compressedBlock
  }

  private def decompressBlock(
      compressedMemoryBlock: MemoryBlock,
      decompressedSize: Int,
      compressionCodec: CompressionCodec): MemoryBlock = {
    // Copy compressed block (excluding padding) to on-heap byte array
    val padding =
      Platform.getInt(compressedMemoryBlock.getBaseObject, compressedMemoryBlock.getBaseOffset)
    val compressedBlockArray = new Array[Byte](compressedMemoryBlock.size().toInt - padding)
    Platform.copyMemory(
      compressedMemoryBlock.getBaseObject,
      compressedMemoryBlock.getBaseOffset + 4,
      compressedBlockArray,
      Platform.BYTE_ARRAY_OFFSET,
      compressedMemoryBlock.size() - padding)

    // Decompress into MemoryBlock backed by on-heap byte array
    val decompressionStream =
      compressionCodec.compressedInputStream(new ByteArrayInputStream(compressedBlockArray))
    val decompressedBlock = new Array[Byte](decompressedSize)
    ByteStreams.readFully(decompressionStream, decompressedBlock)
    decompressionStream.close()
    MemoryBlock.fromByteArray(decompressedBlock)
  }
}
