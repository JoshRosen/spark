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

package org.apache.spark.storage;

import org.apache.spark.annotation.Private;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An interface for writing JVM objects to some underlying storage. This interface allows appending
 * data to an existing block, and can guarantee atomicity in the case of faults as it allows the
 * caller to revert partial writes.
 *
 * This interface does not support concurrent writes. Also, once the writer has been opened, it
 * cannot be reopened again.
 */
@Private
public abstract class BlockObjectWriter extends OutputStream {

  public final BlockId blockId;

  protected BlockObjectWriter(BlockId blockId) {
    this.blockId = blockId;
  }

  public abstract BlockObjectWriter open() throws FileNotFoundException;

  public abstract void close() throws IOException;

  public abstract boolean isOpen();

  /**
   * Flush the partial writes and commit them as a single atomic block.
   */
  public abstract void commitAndClose() throws IOException;

  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   */
  public abstract void revertPartialWritesAndClose();

  /**
   * Writes a key-value pair.
   */
  public abstract void write(Object key, Object value) throws IOException;

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  public abstract void recordWritten() throws IOException;

  /**
   * Returns the file segment of committed data that this Writer has written.
   * This is only valid after {@link BlockObjectWriter#commitAndClose()} has been called.
   */
  public abstract FileSegment fileSegment();
}
