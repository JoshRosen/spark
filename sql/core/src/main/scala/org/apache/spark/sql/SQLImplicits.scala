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

package org.apache.spark.sql

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import com.google.common.io.ByteStreams

import org.apache.spark.SparkEnv
import org.apache.spark.annotation.Experimental
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{SpecificMutableRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.{MemoryBlock, TaskMemoryManager}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A collection of implicit methods for converting common Scala objects into [[DataFrame]]s.
 */
private[sql] abstract class SQLImplicits {
  protected def _sqlContext: SQLContext

  /**
   * An implicit conversion that turns a Scala `Symbol` into a [[Column]].
   * @since 1.3.0
   */
  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

  /**
   * Creates a DataFrame from an RDD of Product (e.g. case classes, tuples).
   * @since 1.3.0
   */
  implicit def rddToDataFrameHolder[A <: Product : TypeTag](rdd: RDD[A]): DataFrameHolder = {
    DataFrameHolder(_sqlContext.createDataFrame(rdd))
  }

  /**
   * Creates a DataFrame from a local Seq of Product.
   * @since 1.3.0
   */
  implicit def localSeqToDataFrameHolder[A <: Product : TypeTag](data: Seq[A]): DataFrameHolder =
  {
    DataFrameHolder(_sqlContext.createDataFrame(data))
  }

  // Do NOT add more implicit conversions. They are likely to break source compatibility by
  // making existing implicit conversions ambiguous. In particular, RDD[Double] is dangerous
  // because of [[DoubleRDDFunctions]].

  /**
   * Creates a single column DataFrame from an RDD[Int].
   * @since 1.3.0
   */
  implicit def intRddToDataFrameHolder(data: RDD[Int]): DataFrameHolder = {
    val dataType = IntegerType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.setInt(0, v)
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }

  /**
   * Creates a single column DataFrame from an RDD[Long].
   * @since 1.3.0
   */
  implicit def longRddToDataFrameHolder(data: RDD[Long]): DataFrameHolder = {
    val dataType = LongType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.setLong(0, v)
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }

  /**
   * Creates a single column DataFrame from an RDD[String].
   * @since 1.3.0
   */
  implicit def stringRddToDataFrameHolder(data: RDD[String]): DataFrameHolder = {
    val dataType = StringType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.update(0, UTF8String.fromString(v))
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }

  /**
   * ::Experimental::
   *
   * Pimp my library decorator for tungsten caching of DataFrames.
   * @since 1.5.1
   */
  @Experimental
  implicit class TungstenCache(df: DataFrame) {
    /**
     * Packs the rows of [[df]] into contiguous blocks of memory.
     * @param compressionType "" (default), "lz4", "lzf", or "snappy", see
     *                        [[CompressionCodec.ALL_COMPRESSION_CODECS]]
     * @param blockSize size of each MemoryBlock (default = 4 MB)
     */
    def tungstenCache(
        compressionType: String = "",
        blockSize: Int = 4000000): (RDD[_], DataFrame) = {
      val schema = df.schema

      val convert = CatalystTypeConverters.createToCatalystConverter(schema)
      val internalRows = df.rdd.map(convert(_).asInstanceOf[InternalRow])
      val cachedRDD = internalRows.mapPartitions { rowIterator =>
        val bufferedRowIterator = rowIterator.buffered
        val convertToUnsafe = UnsafeProjection.create(schema)
        val taskMemoryManager = new TaskMemoryManager(SparkEnv.get.executorMemoryManager)
        val compressionCodec: Option[CompressionCodec] = if (compressionType.isEmpty) {
          None
        } else {
          Some(CompressionCodec.createCodec(SparkEnv.get.conf, compressionType))
        }

        new Iterator[MemoryBlock] {
          // NOTE: This assumes that size of every row < blockSize
          def next(): MemoryBlock = {
            // Packs rows into a `blockSize` bytes contiguous block of memory, starting a new block
            // whenever the current fills up
            // Each row is laid out in memory as [rowSize (4)|row (rowSize)]
            val block = taskMemoryManager.allocateUnchecked(blockSize)

            var currOffset = 0
            val seenRows = ArrayBuffer[InternalRow]()
            while (bufferedRowIterator.hasNext && currOffset < blockSize) {
              val currRow = convertToUnsafe.apply(bufferedRowIterator.head)
              val recordSize = 4 + currRow.getSizeInBytes
              if (currOffset + recordSize < blockSize) {
                Platform.putInt(
                  block.getBaseObject, block.getBaseOffset + currOffset, currRow.getSizeInBytes)
                currRow.writeToMemory(block.getBaseObject, block.getBaseOffset + currOffset + 4)
                seenRows += currRow
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
      }.setName(compressionType + "_" + df.toString).persist(StorageLevel.MEMORY_ONLY)

      val baseRelation: BaseRelation = new BaseRelation with TableScan {
        override val sqlContext = _sqlContext
        override val schema = df.schema
        override val needConversion = false

        override def buildScan(): RDD[Row] = {
          val numFields = this.schema.length
          val _compressionType: String = compressionType
          val _blockSize = blockSize

          cachedRDD.flatMap { rawBlock =>
            // Optionally decompress block
            val compressionCodec: Option[CompressionCodec] = if (_compressionType.isEmpty) {
              None
            } else {
              Some(CompressionCodec.createCodec(SparkEnv.get.conf, _compressionType))
            }
            val block = compressionCodec match {
              case Some(codec) =>
                // Copy compressed block (excluding padding) to on-heap byte array
                val padding = Platform.getInt(rawBlock.getBaseObject, rawBlock.getBaseOffset)
                val compressedBlockArray = new Array[Byte](_blockSize)
                Platform.copyMemory(
                  rawBlock.getBaseObject,
                  rawBlock.getBaseOffset + 4,
                  compressedBlockArray,
                  Platform.BYTE_ARRAY_OFFSET,
                  rawBlock.size() - padding)

                // Decompress into MemoryBlock backed by on-heap byte array
                val decompressionStream =
                  codec.compressedInputStream(new ByteArrayInputStream(compressedBlockArray))
                val decompressedBlock = new Array[Byte](_blockSize)
                ByteStreams.readFully(decompressionStream, decompressedBlock)
                decompressionStream.close()
                MemoryBlock.fromByteArray(decompressedBlock)
              case None => rawBlock
            }

            val rows = new ArrayBuffer[InternalRow]()
            var currOffset = 0
            var moreData = true
            while (currOffset < block.size() && moreData) {
              val rowSize = Platform.getInt(block.getBaseObject, block.getBaseOffset + currOffset)
              currOffset += 4
              // TODO: should probably have a null terminator rather than relying on zeroed out
              if (rowSize > 0) {
                val unsafeRow = new UnsafeRow()
                unsafeRow.pointTo(
                  block.getBaseObject, block.getBaseOffset + currOffset, numFields, rowSize)
                rows.append(unsafeRow)
                currOffset += rowSize
              } else {
                moreData = false
              }
            }
            rows
          }.asInstanceOf[RDD[Row]]
        }

        override def toString: String = getClass.getSimpleName + s"[${df.toString}]"
      }
      cachedRDD.count // trigger caching action because it's lazy
      (cachedRDD, DataFrame(_sqlContext, LogicalRelation(baseRelation)))
    }
  }
}
