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

import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, SparkConf, TaskContext}
import org.apache.spark.memory.MemoryManager
import org.apache.spark.storage.{BlockId, BlockManager, StorageLevel}
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector

private case class MemoryEntry(value: Any, size: Long, deserialized: Boolean)

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private[spark] class MemoryStore(
    conf: SparkConf,
    blockManager: BlockManager,
    memoryManager: MemoryManager)
  extends Logging {

  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!

  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)

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
  def putBytes(blockId: BlockId, size: Long, _bytes: () => ByteBuffer): Boolean = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    if (memoryManager.acquireStorageMemory(blockId, size)) {
      // We acquired enough memory for the block, so go ahead and put it
      // Work on a duplicate - since the original input might be used elsewhere.
      val bytes = _bytes().duplicate().rewind().asInstanceOf[ByteBuffer]
      assert(bytes.limit == size)
      val entry = new MemoryEntry(bytes, size, deserialized = false)
      entries.synchronized {
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(size), Utils.bytesToString(blocksMemoryUsed)))
      true
    } else {
      false
    }
  }

  /**
   * Attempt to put the given block in memory store.
   *
   * @return the estimated size of the stored data if the put() succeeded, or an iterator
   *         in case the put() failed (the returned iterator lets callers fall back to the disk
   *         store if desired).
   */
  private[storage] def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel): Either[(Long, Iterator[Any]), Long] = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    // Number of elements unrolled so far
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes). Exposed for testing.
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = 16
    // Memory currently reserved by this task for this particular unrolling operation
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Keep track of pending unroll memory reserved by this method.
    var pendingMemoryReserved = 0L
    // Underlying vector for unrolling the block
    var vector = new SizeTrackingVector[Any]

    // Request enough memory to begin unrolling
    keepUnrolling = reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      pendingMemoryReserved += initialMemoryThreshold
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
            pendingMemoryReserved += amountToRequest
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
        val sizeEstimate = SizeEstimator.estimate(arrayValues.asInstanceOf[AnyRef])
        new MemoryEntry(arrayValues, sizeEstimate, deserialized = true)
      } else {
        val bytes = blockManager.dataSerialize(blockId, arrayValues.iterator)
        new MemoryEntry(bytes, bytes.limit, deserialized = false)
      }
      val size = entry.size
      val acquiredStorageMemory = {
        if (pendingMemoryReserved == size) {
          true
        } else if (pendingMemoryReserved < size) {
          memoryManager.acquireStorageMemory(blockId, size - pendingMemoryReserved)
        } else {
          memoryManager.releaseStorageMemory(pendingMemoryReserved - size)
          true
        }
      }
      if (acquiredStorageMemory) {
        // We acquired enough memory for the block, so go ahead and put it
        memoryManager.synchronized {
          unrollMemoryMap(currentTaskAttemptId()) -= pendingMemoryReserved
        }
        entries.synchronized {
          entries.put(blockId, entry)
        }
        val bytesOrValues = if (level.deserialized) "values" else "bytes"
        logInfo("Block %s stored as %s in memory (estimated size %s, free %s)".format(
          blockId, bytesOrValues, Utils.bytesToString(size), Utils.bytesToString(blocksMemoryUsed)))
        Right(size)
      } else {
        Left((pendingMemoryReserved, arrayValues.toIterator))
      }
    } else {
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, vector.estimateSize())
      Left(pendingMemoryReserved, vector.iterator ++ values)
    }
  }

  def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else {
      require(!entry.deserialized, "should only call getBytes on blocks stored in serialized form")
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate()) // Doesn't actually copy the data
    }
  }

  def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else {
      require(entry.deserialized, "should only call getValues on deserialized blocks")
      Some(entry.value.asInstanceOf[Array[Any]].iterator)
    }
  }

  def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    val entry = entries.synchronized {
      entries.remove(blockId)
    }
    if (entry != null) {
      memoryManager.releaseStorageMemory(entry.size)
      logDebug(s"Block $blockId of size ${entry.size} dropped " +
        s"from memory (free ${maxMemory - blocksMemoryUsed})")
      true
    } else {
      false
    }
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
    * @param space the size of this block
    * @return the amount of memory (in bytes) freed by eviction
    */
  private[spark] def evictBlocksToFreeSpace(blockId: Option[BlockId], space: Long): Long = {
    assert(space > 0)
    memoryManager.synchronized {
      var freedMemory = 0L
      val rddToAdd = blockId.flatMap(getRddId)
      val selectedBlocks = new ArrayBuffer[BlockId]
      def blockIsEvictable(blockId: BlockId): Boolean = {
        rddToAdd.isEmpty || rddToAdd != getRddId(blockId)
      }
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        val iterator = entries.entrySet().iterator()
        while (freedMemory < space && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair.getKey
          if (blockIsEvictable(blockId)) {
            // We don't want to evict blocks which are currently being read, so we need to obtain
            // an exclusive write lock on blocks which are candidates for eviction. We perform a
            // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
            if (blockManager.blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
              selectedBlocks += blockId
              freedMemory += pair.getValue.size
            }
          }
        }
      }

      if (freedMemory >= space) {
        logInfo(s"${selectedBlocks.size} blocks selected for dropping")
        for (blockId <- selectedBlocks) {
          val entry = entries.synchronized { entries.get(blockId) }
          // This should never be null as only one task should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry != null) {
            val data = if (entry.deserialized) {
              Left(entry.value.asInstanceOf[Array[Any]])
            } else {
              Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
            }
            val newEffectiveStorageLevel = blockManager.dropFromMemory(blockId, () => data)
            if (newEffectiveStorageLevel.isValid) {
              // The block is still present in at least one store, so release the lock
              // but don't delete the block info
              blockManager.releaseLock(blockId)
            } else {
              // The block isn't present in any store, so delete the block info so that the
              // block can be stored again
              blockManager.blockInfoManager.removeBlock(blockId)
            }
          }
        }
        freedMemory
      } else {
        blockId.foreach { id =>
          logInfo(s"Will not store $id as it would require dropping another block " +
            "from the same RDD")
        }
        selectedBlocks.foreach { id =>
          blockManager.releaseLock(id)
        }
        0L
      }
    }
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
    memoryManager.synchronized {
      val success = memoryManager.acquireUnrollMemory(blockId, memory)
      if (success) {
        val taskAttemptId = currentTaskAttemptId()
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      success
    }
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
