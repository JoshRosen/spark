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

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

class KeyValueSerializer(wrapped: Serializer) extends Serializer with Serializable {
  override private[spark] def supportsRelocationOfSerializedObjects: Boolean =
    wrapped.supportsRelocationOfSerializedObjects
  override def newInstance(): SerializerInstance =
    new KeyValueSerializerInstance(wrapped.newInstance())
}

class KeyValueSerializerInstance(wrapped: SerializerInstance) extends SerializerInstance {
  override def serialize[T: ClassTag](obj: T): ByteBuffer = wrapped.serialize(obj)
  override def serializeStream(s: OutputStream): SerializationStream =
    new KeyValueSerializationStream(wrapped.serializeStream(s))
  override def deserializeStream(s: InputStream): DeserializationStream =
    new KeyValueDeserializationStream(wrapped.deserializeStream(s))
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = wrapped.deserialize(bytes)
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    wrapped.deserialize(bytes, loader)
}

class KeyValueSerializationStream(wrapped: SerializationStream) extends SerializationStream {
  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    val pair = t.asInstanceOf[Product2[Any, Any]]
    wrapped.writeObject(pair._1)
    wrapped.writeObject(pair._2)
  }
  override def flush(): Unit = wrapped.flush()
  override def close(): Unit = wrapped.close()
}

class KeyValueDeserializationStream(wrapped: DeserializationStream) extends DeserializationStream {
  override def readObject[T: ClassTag](): T = {
    (wrapped.readObject(), wrapped.readObject()).asInstanceOf[T]
  }
  override def close(): Unit = wrapped.close()
}
