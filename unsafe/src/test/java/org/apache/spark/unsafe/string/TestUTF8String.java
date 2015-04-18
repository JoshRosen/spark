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

package org.apache.spark.unsafe.string;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.MemoryBlock;
import java.lang.String;

public class TestUTF8String {

  @Test
  public void toStringTest() {
    final String javaStr = "Hello, World!";
    final byte[] javaStrBytes = javaStr.getBytes();
    final int paddedSizeInWords = javaStrBytes.length / 8 + (javaStrBytes.length % 8 == 0 ? 0 : 1);
    final MemoryLocation memory = MemoryBlock.fromLongArray(new long[paddedSizeInWords]);
    final long bytesWritten = UTF8StringMethods.createFromJavaString(
      memory.getBaseObject(),
      memory.getBaseOffset(),
      javaStr);
    Assert.assertEquals(javaStrBytes.length, bytesWritten);
    final UTF8StringPointer utf8String = new UTF8StringPointer();
    utf8String.set(memory.getBaseObject(), memory.getBaseOffset(), bytesWritten);
    Assert.assertEquals(javaStr, utf8String.toJavaString());
  }
}
