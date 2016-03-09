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

package org.apache.spark.serializer

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import com.google.common.io.ByteStreams


private[serializer] object ByteBufferSerializer extends Serializer {
  override private[spark] def supportsRelocationOfSerializedObjects: Boolean = true
  override def newInstance(): SerializerInstance = ByteBufferSerializerInstance
}

private object ByteBufferSerializerInstance extends SerializerInstance {
  override def serialize[T: ClassTag](t: T): ByteBuffer = t.asInstanceOf[ByteBuffer]
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = bytes.asInstanceOf[T]
  override def deserialize[T: ClassTag](b: ByteBuffer, l: ClassLoader): T = deserialize(b)
  override def serializeStream(os: OutputStream): SerializationStream =
    new ByteBufferSerializationStream(new DataOutputStream(os))
  override def deserializeStream(is: InputStream): DeserializationStream =
    new ByteBufferDeserializationStream(new DataInputStream(is))
}

private class ByteBufferSerializationStream(os: DataOutputStream) extends SerializationStream {
  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    val buffer = t.asInstanceOf[ByteBuffer]
    buffer.rewind()
    os.writeInt(buffer.capacity())
    os.write(buffer.array())
    this
  }
  override def flush(): Unit = os.flush()
  override def close(): Unit = os.close()
}

private class ByteBufferDeserializationStream(is: DataInputStream) extends DeserializationStream {
  override def readObject[T: ClassTag](): T = {
    val capacity = is.readInt()
    val arr = new Array[Byte](capacity)
    ByteStreams.readFully(is, arr)
    ByteBuffer.wrap(arr).asInstanceOf[T]
  }
  override def close(): Unit = is.close()
}