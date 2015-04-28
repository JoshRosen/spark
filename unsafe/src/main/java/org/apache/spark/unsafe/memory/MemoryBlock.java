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

package org.apache.spark.unsafe.memory;

import javax.annotation.Nullable;

import org.apache.spark.unsafe.PlatformDependent;

/**
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 */
public class MemoryBlock extends MemoryLocation {

  private final long length;

  /**
   * Optional page number; used when this MemoryBlock represents a page allocated by a
   * MemoryManager. This is package-private and is modified by MemoryManager.
   */
  int pageNumber = -1;

  public int getPageNumber() {
    return pageNumber;
  }

  MemoryBlock(@Nullable Object obj, long offset, long length) {
    super(obj, offset);
    this.length = length;
  }

  /**
   * Returns the size of the memory block.
   */
  public long size() {
    return length;
  }

  /**
   * Clear the contents of this memory block.  Returns `this` to facilitate chaining.
   */
  public MemoryBlock zero() {
    PlatformDependent.UNSAFE.setMemory(obj, offset, length, (byte) 0);
    return this;
  }

  /**
   * Creates a memory block pointing to the memory used by the byte array.
   */
  public static MemoryBlock fromByteArray(final byte[] array) {
    return new MemoryBlock(array, PlatformDependent.BYTE_ARRAY_OFFSET, array.length);
  }

  /**
   * Creates a memory block pointing to the memory used by the long array.
   */
  public static MemoryBlock fromLongArray(final long[] array) {
    return new MemoryBlock(array, PlatformDependent.LONG_ARRAY_OFFSET, array.length * 8);
  }
}
