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

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform

/**
 * Serializer for serializing [[UnsafeRow]]s during shuffle. Since UnsafeRows are already stored as
 * bytes, this serializer simply copies those bytes to the underlying output stream. When
 * deserializing a stream of rows, instances of this serializer mutate and return a single UnsafeRow
 * instance that is backed by an on-heap byte array.
 *
 * Note that this serializer implements only the [[Serializer]] methods that are used during
 * shuffle, so certain [[SerializerInstance]] methods will throw UnsupportedOperationException.
 *
 * @param numFields the number of fields in the row being serialized.
 */
private[sql] class UnsafeRowSerializer(numFields: Int) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new UnsafeRowSerializerInstance(numFields)
  override private[spark] def supportsRelocationOfSerializedObjects: Boolean = true
}

private class UnsafeRowSerializerInstance(numFields: Int) extends SerializerInstance {
  /**
   * Serializes a stream of UnsafeRows. Within the stream, each record consists of a record
   * length (stored as a 4-byte integer, written high byte first), followed by the record's bytes.
   */
  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] var writeBuffer: Array[Byte] = new Array[Byte](4096)
    private[this] val dOut: DataOutputStream =
      new DataOutputStream(new BufferedOutputStream(out))

    override def writeObject[T: ClassTag](value: T): SerializationStream = {
      val row = value.asInstanceOf[UnsafeRow]

      dOut.writeInt(row.getSizeInBytes)
      row.writeToStream(dOut, writeBuffer)
      this
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      writeBuffer = null
      dOut.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))
      // 1024 is a default buffer size; this buffer will grow to accommodate larger rows
      private[this] var rowBuffer: Array[Byte] = new Array[Byte](1024)
      private[this] var row: UnsafeRow = new UnsafeRow(numFields)
      private[this] val EOF: Int = -1

      override def asIterator[T: ClassTag]: Iterator[T] = {
        new Iterator[T] {

          private[this] def readSize(): Int = try {
            dIn.readInt()
          } catch {
            case e: EOFException =>
              dIn.close()
              EOF
          }

          private[this] var rowSize: Int = readSize()
          override def hasNext: Boolean = rowSize != EOF

          override def next(): T = {
            if (rowBuffer.length < rowSize) {
              rowBuffer = new Array[Byte](rowSize)
            }
            ByteStreams.readFully(dIn, rowBuffer, 0, rowSize)
            row.pointTo(rowBuffer, Platform.BYTE_ARRAY_OFFSET, rowSize)
            rowSize = readSize()
            if (rowSize == EOF) { // We are returning the last row in this stream
              dIn.close()
              val _row = row
              // Null these out so that the byte array can be garbage collected once the entire
              // iterator has been consumed
              row = null
              rowBuffer = null
              _row.asInstanceOf[T]
            } else {
              row.asInstanceOf[T]
            }
          }
        }
      }

      override def readObject[T: ClassTag](): T = {
        val rowSize = dIn.readInt()
        if (rowBuffer.length < rowSize) {
          rowBuffer = new Array[Byte](rowSize)
        }
        ByteStreams.readFully(dIn, rowBuffer, 0, rowSize)
        row.pointTo(rowBuffer, Platform.BYTE_ARRAY_OFFSET, rowSize)
        row.asInstanceOf[T]
      }

      override def close(): Unit = {
        dIn.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
