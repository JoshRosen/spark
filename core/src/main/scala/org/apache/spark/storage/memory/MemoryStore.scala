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

package org.apache.spark.storage.memory

import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryManager
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockId, BlockInfoManager, StorageLevel}
import org.apache.spark.util.{CompletionIterator, SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector
import org.apache.spark.util.io.ChunkedByteBuffer

private sealed trait MemoryEntry[T] {
  def size: Long
  def classTag: ClassTag[T]
}
private case class DeserializedMemoryEntry[T](
    value: Array[T],
    size: Long,
    classTag: ClassTag[T]) extends MemoryEntry[T]
private case class SerializedMemoryEntry[T](
    buffer: ChunkedByteBuffer,
    size: Long,
    classTag: ClassTag[T]) extends MemoryEntry[T]

private[storage] trait BlockEvictionHandler {
  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   *
   * @return the block's new effective StorageLevel.
   */
  private[storage] def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel
}

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private[spark] class MemoryStore(
    conf: SparkConf,
    blockInfoManager: BlockInfoManager,
    serializerManager: SerializerManager,
    memoryManager: MemoryManager,
    blockEvictionHandler: BlockEvictionHandler)
  extends Logging {

  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!

  private val entries = new LinkedHashMap[BlockId, MemoryEntry[_]](32, 0.75f, true)

  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
  private val unrollMemoryMap = mutable.HashMap[Long, Long]()

  // Initial memory to request before unrolling any block
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

  /** Total amount of memory available for storage, in bytes. */
  private def maxMemory: Long = memoryManager.maxStorageMemory

  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /** Total storage memory used including unroll memory, in bytes. */
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
   * Amount of storage memory, in bytes, used for caching blocks.
   * This does not include memory used for unrolling.
   */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    memoryUsed - currentUnrollMemory
  }

  def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  /**
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
   *
   * @return true if the put() succeeded, false otherwise.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      size: Long,
      _bytes: () => ChunkedByteBuffer): Boolean = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    if (memoryManager.acquireStorageMemory(blockId, size)) {
      // We acquired enough memory for the block, so go ahead and put it
      val bytes = _bytes()
      assert(bytes.size == size)
      val entry = new SerializedMemoryEntry[T](bytes, size, implicitly[ClassTag[T]])
      entries.synchronized {
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      true
    } else {
      false
    }
  }

  /**
   * Attempt to put the given block in memory store.
   *
   * It's possible that the iterator is too large to materialize and store in memory. To avoid
   * OOM exceptions, this method will gradually unroll the iterator while periodically checking
   * whether there is enough free memory. If the block is successfully materialized, then the
   * temporary unroll memory used during the materialization is "transferred" to storage memory,
   * so we won't acquire more memory than is actually needed to store the block.
   *
   * @return in case of success, the estimated the estimated size of the stored data. In case of
   *         failure, return an iterator containing the values of the block. The returned iterator
   *         will be backed by the combination of the partially-unrolled block and the remaining
   *         elements of the original input iterator. The caller must either fully consume this
   *         iterator or call `close()` on it in order to free the storage memory consumed by the
   *         partially-unrolled block.
   */
  private[storage] def putIterator[T](
      blockId: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // Number of elements unrolled so far
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes).
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = 16
    // Memory currently reserved by this task for this particular unrolling operation
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Keep track of unroll memory used by this particular block / putIterator() operation
    var unrollMemoryUsedByThisBlock = 0L
    // Underlying vector for unrolling the block
    var vector = new SizeTrackingVector[T]()(classTag)

    // Request enough memory to begin unrolling
    keepUnrolling = reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    while (values.hasNext && keepUnrolling) {
      vector += values.next()
      if (elementsUnrolled % memoryCheckPeriod == 0) {
        // If our vector's size has exceeded the threshold, request more memory
        val currentSize = vector.estimateSize()
        if (currentSize >= memoryThreshold) {
          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest)
          if (keepUnrolling) {
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // New threshold is currentSize * memoryGrowthFactor
          memoryThreshold += amountToRequest
        }
      }
      elementsUnrolled += 1
    }

    if (keepUnrolling) {
      // We successfully unrolled the entirety of this block
      val arrayValues = vector.toArray
      vector = null
      val entry = if (level.deserialized) {
        new DeserializedMemoryEntry[T](arrayValues, SizeEstimator.estimate(arrayValues), classTag)
      } else {
        val bytes = serializerManager.dataSerialize(blockId, arrayValues.iterator)(classTag)
        new SerializedMemoryEntry[T](bytes, bytes.size, classTag)
      }
      val size = entry.size
      def transferUnrollToStorage(amount: Long): Unit = {
        // Synchronize so that transfer is atomic
        memoryManager.synchronized {
          releaseUnrollMemoryForThisTask(amount)
          val success = memoryManager.acquireStorageMemory(blockId, amount)
          assert(success, "transferring unroll memory to storage memory failed")
        }
      }
      // Acquire storage memory if necessary to store this block in memory.
      val enoughStorageMemory = {
        if (unrollMemoryUsedByThisBlock <= size) {
          val acquiredExtra =
            memoryManager.acquireStorageMemory(blockId, size - unrollMemoryUsedByThisBlock)
          if (acquiredExtra) {
            transferUnrollToStorage(unrollMemoryUsedByThisBlock)
          }
          acquiredExtra
        } else { // unrollMemoryUsedByThisBlock > size
          // If this task attempt already owns more unroll memory than is necessary to store the
          // block, then release the extra memory that will not be used.
          val excessUnrollMemory = unrollMemoryUsedByThisBlock - size
          releaseUnrollMemoryForThisTask(excessUnrollMemory)
          transferUnrollToStorage(size)
          true
        }
      }
      if (enoughStorageMemory) {
        entries.synchronized {
          entries.put(blockId, entry)
        }
        val bytesOrValues = if (level.deserialized) "values" else "bytes"
        logInfo("Block %s stored as %s in memory (estimated size %s, free %s)".format(
          blockId,
          bytesOrValues,
          Utils.bytesToString(size),
          Utils.bytesToString(maxMemory - blocksMemoryUsed)))
        Right(size)
      } else {
        assert(currentUnrollMemoryForThisTask >= currentUnrollMemoryForThisTask,
          "released too much unroll memory")
        Left(new PartiallyUnrolledIterator(
          this,
          unrollMemoryUsedByThisBlock,
          unrolled = arrayValues.toIterator,
          rest = Iterator.empty))
      }
    } else {
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, vector.estimateSize())
      Left(new PartiallyUnrolledIterator(
        this, unrollMemoryUsedByThisBlock, unrolled = vector.iterator, rest = values))
    }
  }

  def getBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: DeserializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getBytes on serialized blocks")
      case SerializedMemoryEntry(bytes, _, _) => Some(bytes)
    }
  }

  def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: SerializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getValues on deserialized blocks")
      case DeserializedMemoryEntry(values, _, _) =>
        val x = Some(values)
        x.map(_.iterator)
    }
  }

  def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    val entry = entries.synchronized {
      entries.remove(blockId)
    }
    if (entry != null) {
      memoryManager.releaseStorageMemory(entry.size)
      logInfo(s"Block $blockId of size ${entry.size} dropped " +
        s"from memory (free ${maxMemory - blocksMemoryUsed})")
      true
    } else {
      false
    }
  }

  private[storage] def removeBlockAndKeepStorageMemory(blockId: BlockId): Long = {
    val entry = entries.synchronized {
      entries.remove(blockId)
    }
    entry.size
  }

  def clear(): Unit = memoryManager.synchronized {
    entries.synchronized {
      entries.clear()
    }
    unrollMemoryMap.clear()
    memoryManager.releaseAllStorageMemory()
    logInfo("MemoryStore cleared")
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  /**
    * Try to evict blocks to free up a given amount of space to store a particular block.
    * Can fail if either the block is bigger than our memory or it would require replacing
    * another block from the same RDD (which leads to a wasteful cyclic replacement pattern for
    * RDDs that don't fit into memory that we want to avoid).
    *
    * @param blockId the ID of the block we are freeing space for, if any
    * @param requestedSpace the amount of memory which should be freed via block eviction
    * @return the total size (in bytes) of the blocks evicted from the MemoryStore. This memory is
    *         not released with the memory manager, so it's the caller's responsibility to perform
    *         the appropriate bookkeeping calls.
    */
  private[spark] def evictBlocksToFreeSpace(
      blockId: Option[BlockId],
      requestedSpace: Long): Long = {
    assert(requestedSpace > 0)

    var freedMemory = 0L
    val rddToAdd = blockId.flatMap(getRddId)
    val selectedBlocks = new ArrayBuffer[BlockId]

    def blockIsEvictable(blockId: BlockId): Boolean = {
      rddToAdd.isEmpty || rddToAdd != getRddId(blockId)
    }

    def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit = {
      val data = entry match {
        case DeserializedMemoryEntry(values, _, _) => Left(values)
        case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
      }
      val newEffectiveStorageLevel =
        blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
      if (newEffectiveStorageLevel.isValid) {
        // The block is still present in at least one store, so release the lock
        // but don't delete the block info
        blockInfoManager.unlock(blockId)
      } else {
        // The block isn't present in any store, so delete the block info so that the
        // block can be stored again
        blockInfoManager.removeBlock(blockId)
      }
    }

    // First, determine the set of blocks to evict. This requires synchronization so that different
    // tasks don't try to evict the same block.
    memoryManager.synchronized {
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        val iterator = entries.entrySet().iterator()
        while (freedMemory < requestedSpace && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair.getKey
          if (blockIsEvictable(blockId)) {
            // We don't want to evict blocks which are currently being read, so we need to obtain
            // an exclusive write lock on blocks which are candidates for eviction. We perform a
            // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
            if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
              selectedBlocks += blockId
              freedMemory += pair.getValue.size
            }
          }
        }
      }

      if (freedMemory >= requestedSpace) {
        logInfo(s"${selectedBlocks.size} blocks selected for dropping " +
          s"(${Utils.bytesToString(freedMemory)} bytes)")
      } else {
        blockId.foreach { id =>
          logInfo(s"Will not store $id")
        }
        selectedBlocks.foreach { id =>
          blockInfoManager.unlock(id)
        }
        selectedBlocks.clear()
        freedMemory = 0
      }
    }

    // At this point, `selectedBlocks` is the (possibly-empty) set of blocks that we have chosen
    // to evict and we hold exclusive write locks on all of those blocks. We can now drop the blocks
    // to disk without holding the memoryManager lock because the per-block write locks ensure that
    // threads won't compete to drop the same block.
    for (blockId <- selectedBlocks) {
      val entry = entries.synchronized { entries.get(blockId) }
      dropBlock(blockId, entry)
    }

    freedMemory
  }

  def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Reserve memory for unrolling the given block for this task.
   *
   * @return whether the request is granted.
   */
  def reserveUnrollMemoryForThisTask(blockId: BlockId, memory: Long): Boolean = {
    val success = memoryManager.acquireUnrollMemory(blockId, memory)
    if (success) {
      val taskAttemptId = currentTaskAttemptId()
      memoryManager.synchronized {
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
    }
    success
  }

  /**
   * Release memory used by this task for unrolling blocks.
   * If the amount is not specified, remove the current task's allocation altogether.
   */
  def releaseUnrollMemoryForThisTask(memory: Long = Long.MaxValue): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      if (unrollMemoryMap.contains(taskAttemptId)) {
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          if (unrollMemoryMap(taskAttemptId) == 0) {
            unrollMemoryMap.remove(taskAttemptId)
          }
          memoryManager.releaseUnrollMemory(memoryToRelease)
        }
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
   */
  def currentUnrollMemory: Long = memoryManager.synchronized {
    unrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this task.
   */
  def currentUnrollMemoryForThisTask: Long = memoryManager.synchronized {
    unrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
   * Return the number of tasks currently unrolling blocks.
   */
  private def numTasksUnrolling: Int = memoryManager.synchronized { unrollMemoryMap.keys.size }

  /**
   * Log information about current memory usage.
   */
  private def logMemoryUsage(): Unit = {
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
      s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  private def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}

/**
 * The result of a failed [[MemoryStore.putIterator()]] call.
 *
 * @param memoryStore the memoryStore, used for freeing memory.
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param unrolled an iterator for the partially-unrolled values.
 * @param rest the rest of the original iterator passed to [[MemoryStore.putIterator()]].
 */
private[storage] class PartiallyUnrolledIterator[T](
    memoryStore: MemoryStore,
    unrollMemory: Long,
    unrolled: Iterator[T],
    rest: Iterator[T])
  extends Iterator[T] {

  private[this] var unrolledIteratorIsConsumed: Boolean = false
  private[this] var iter: Iterator[T] = {
    val completionIterator = CompletionIterator[T, Iterator[T]](unrolled, {
      unrolledIteratorIsConsumed = true
      memoryStore.releaseUnrollMemoryForThisTask(unrollMemory)
    })
    completionIterator ++ rest
  }

  override def hasNext: Boolean = iter.hasNext
  override def next(): T = iter.next()

  /**
   * Called to dispose of this iterator and free its memory.
   */
  def close(): Unit = {
    if (!unrolledIteratorIsConsumed) {
      memoryStore.releaseUnrollMemoryForThisTask(unrollMemory)
      unrolledIteratorIsConsumed = true
    }
    iter = null
  }
}
