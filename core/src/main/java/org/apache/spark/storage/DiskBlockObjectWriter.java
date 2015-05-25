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

import java.io.*;
import java.nio.channels.FileChannel;

import scala.Function1;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.io.Closeables;
import org.apache.spark.serializer.SerializationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.SerializerInstance;

/**
 * BlockObjectWriter which writes directly to a file on disk. Appends to the given file.
 */
@Private
public class DiskBlockObjectWriter extends BlockObjectWriter {

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  private final Logger logger = LoggerFactory.getLogger(DiskBlockObjectWriter.class);

  private final File file;
  private final SerializerInstance serializerInstance;
  private final int bufferSize;
  private final Function1<OutputStream, OutputStream> compressStream;
  private final boolean syncWrites;
  /**
   * These write metrics concurrently shared with other active BlockObjectWriter's who
   * are themselves performing writes. All updates must be relative.
   */
  private final ShuffleWriteMetrics writeMetrics;

  /** The file channel, used for repositioning / truncating the file. */
  private FileChannel channel;
  private OutputStream bs;
  private FileOutputStream fos;
  private TimeTrackingOutputStream ts;
  private SerializationStream objOut;
  private boolean initialized = false;
  private boolean hasBeenClosed = false;

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   */
  private long numRecordsWritten;

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxx|--------|---       |
   *         ^        ^          ^
   *         |        |        finalPosition
   *         |      reportedPosition
   *       initialPosition
   *
   * initialPosition: Offset in the file where we start writing. Immutable.
   * reportedPosition: Position at the time of the last update to the write metrics.
   * finalPosition: Offset where we stopped writing. Set on closeAndCommit() then never changed.
   * -----: Current writes to the underlying file.
   * xxxxx: Existing contents of the file.
   */
  private long initialPosition;
  private long finalPosition;
  private long reportedPosition;

  public DiskBlockObjectWriter(
      BlockId blockId,
      File file,
      SerializerInstance serializerInstance,
      int bufferSize,
      Function1<OutputStream, OutputStream> compressStream,
      boolean syncWrites,
      ShuffleWriteMetrics writeMetrics) {
    super(blockId);
    this.file = file;
    this.serializerInstance = serializerInstance;
    this.bufferSize = bufferSize;
    this.compressStream = compressStream;
    this.syncWrites = syncWrites;
    this.writeMetrics = writeMetrics;

    this.initialPosition = file.length();
    this.finalPosition = -1;
    this.reportedPosition = initialPosition;
  }

  @Override
  public BlockObjectWriter open() throws FileNotFoundException {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.");
    }
    fos = new FileOutputStream(file, true);
    ts = new TimeTrackingOutputStream(writeMetrics, fos);
    channel = fos.getChannel();
    bs = compressStream.apply(new BufferedOutputStream(ts, bufferSize));
    objOut = serializerInstance.serializeStream(bs);
    initialized = true;
    return this;
  }

  @Override
  public void close() throws IOException {
    if (!initialized) {
      return;
    }
    boolean threwException = true;
    try {
      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        objOut.flush();
        final long start = System.nanoTime();
        fos.getFD().sync();
        writeMetrics.incShuffleWriteTime(System.nanoTime() - start);
      }
      threwException = false;
    } finally {
      Closeables.close(objOut, threwException);
    }
    channel = null;
    bs = null;
    fos = null;
    ts = null;
    objOut = null;
    initialized = false;
    hasBeenClosed = true;
  }

  @Override
  public boolean isOpen() {
    return objOut != null;
  }

  @Override
  public void commitAndClose() throws IOException {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush();
      bs.flush();
      close();
    }
    finalPosition = file.length();
    // In certain compression codecs, more bytes are written after close() is called
    writeMetrics.incShuffleBytesWritten(finalPosition - reportedPosition);
  }

  @Override
  public void revertPartialWritesAndClose() {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    try {
      writeMetrics.decShuffleBytesWritten(reportedPosition - initialPosition);
      writeMetrics.decShuffleRecordsWritten(numRecordsWritten);

      if (initialized) {
        objOut.flush();
        bs.flush();
        close();
      }

      final FileOutputStream truncateStream = new FileOutputStream(file, true);
      try {
        truncateStream.getChannel().truncate(initialPosition);
      } finally {
        truncateStream.close();
      }
    } catch (Exception e) {
      logger.error("Uncaught exception while reverting partial writes to file " + file, e);
    }
  }

  @Override
  public void write(int b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] kvBytes, int off, int len) throws IOException {
    if (!initialized) {
      open();
    }

    bs.write(kvBytes, off, len);
  }

  @Override
  public void write(Object key, Object value) throws IOException {
    if (!initialized) {
      open();
    }

    objOut.writeKey(key, OBJECT_CLASS_TAG);
    objOut.writeValue(value, OBJECT_CLASS_TAG);
    recordWritten();
  }

  @Override
  public void recordWritten() throws IOException {
    numRecordsWritten += 1;
    writeMetrics.incShuffleRecordsWritten(1);

    if (numRecordsWritten % 32 == 0) {
      updateBytesWritten();
    }
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private void updateBytesWritten() throws IOException {
    long pos = channel.position();
    writeMetrics.incShuffleBytesWritten(pos - reportedPosition);
    reportedPosition = pos;
  }

  @Override
  public FileSegment fileSegment() {
    return new FileSegment(file, initialPosition, finalPosition - initialPosition);
  }

  @Override
  public void flush() throws IOException {
    objOut.flush();
    bs.flush();
  }
}
