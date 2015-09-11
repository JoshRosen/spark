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
import org.apache.spark.unsafe.memory.MemoryBlock
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
    val numFields = child.schema.length
    val childRDD: RDD[UnsafeRow] = {
      if (child.outputsUnsafeRows) {
        child.execute().asInstanceOf[RDD[UnsafeRow]]
      } else {
        ConvertToUnsafe(child).execute().asInstanceOf[RDD[UnsafeRow]]
      }
    }
    val cachedRDD: RDD[MemoryBlock] = childRDD.mapPartitions { rowIterator =>
      val bufferedRowIterator = rowIterator.buffered
      val taskMemoryManager = TaskContext.get().taskMemoryManager()
      val compressionCodec: Option[CompressionCodec] =
        compressionType.map { t => CompressionCodec.createCodec(SparkEnv.get.conf, t) }
      new Iterator[MemoryBlock] {
        // NOTE: This assumes that size of every row < blockSize
        def next(): MemoryBlock = {
          // Packs rows into a `blockSize` bytes contiguous block of memory, starting a new block
          // whenever the current fills up
          // Each row is laid out in memory as [rowSize (4)|row (rowSize)]
          val block = taskMemoryManager.allocateUnchecked(blockSize)

          var currOffset = 0
          while (bufferedRowIterator.hasNext && currOffset < blockSize) {
            val currRow = bufferedRowIterator.head
            val recordSize = 4 + currRow.getSizeInBytes
            if (currOffset + recordSize < blockSize) {
              Platform.putInt(
                block.getBaseObject, block.getBaseOffset + currOffset, currRow.getSizeInBytes)
              currRow.writeToMemory(block.getBaseObject, block.getBaseOffset + currOffset + 4)
              bufferedRowIterator.next()
            }
            currOffset += recordSize // Increment currOffset regardless to break loop when full
          }

          // Optionally compress block before writing
          compressionCodec match {
            case Some(codec) =>
              // Compress the block using an on-heap byte array
              val compressedBlockArray: Array[Byte] = {
                val blockArray = new Array[Byte](blockSize)
                Platform.copyMemory(
                  block.getBaseObject,
                  block.getBaseOffset,
                  blockArray,
                  Platform.BYTE_ARRAY_OFFSET,
                  blockSize)
                val baos = new ByteArrayOutputStream(blockSize)
                val compressedBaos = codec.compressedOutputStream(baos)
                compressedBaos.write(blockArray)
                compressedBaos.flush()
                compressedBaos.close()
                baos.toByteArray
              }

              // Allocate a new block with compressed byte array padded to word boundary
              val totalRecordSize = compressedBlockArray.length + 4
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
              taskMemoryManager.freeUnchecked(block)
              compressedBlock
            case None => block
          }
        }

        def hasNext: Boolean = bufferedRowIterator.hasNext
      }
    }.setName(compressionType + "_" + child.nodeName).persist(StorageLevel.MEMORY_ONLY)
    // TODO(josh): is this the right name?

    cachedRDD.mapPartitions { blockIterator =>
      blockIterator.flatMap { rawBlock =>
        // Optionally decompress block
        val compressionCodec: Option[CompressionCodec] =
          compressionType.map { t => CompressionCodec.createCodec(SparkEnv.get.conf, t) }
        val block: MemoryBlock = compressionCodec match {
          case Some(codec) =>
            // Copy compressed block (excluding padding) to on-heap byte array
            val padding = Platform.getInt(rawBlock.getBaseObject, rawBlock.getBaseOffset)
            val compressedBlockArray = new Array[Byte](blockSize)
            Platform.copyMemory(
              rawBlock.getBaseObject,
              rawBlock.getBaseOffset + 4,
              compressedBlockArray,
              Platform.BYTE_ARRAY_OFFSET,
              rawBlock.size() - padding)

            // Decompress into MemoryBlock backed by on-heap byte array
            val decompressionStream =
              codec.compressedInputStream(new ByteArrayInputStream(compressedBlockArray))
            val decompressedBlock = new Array[Byte](blockSize)
            ByteStreams.readFully(decompressionStream, decompressedBlock)
            decompressionStream.close()
            MemoryBlock.fromByteArray(decompressedBlock)
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
          override def hasNext: Boolean = _nextRowSize > 0
          override def next(): UnsafeRow = {
            // TODO: should probably have a null terminator rather than relying on zeroed out
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
}
