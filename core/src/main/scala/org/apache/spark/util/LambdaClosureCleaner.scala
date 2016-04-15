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

package org.apache.spark.util

import java.lang.reflect.Method

import org.apache.xbean.asm5.{ClassVisitor, MethodVisitor}
import org.apache.xbean.asm5.Opcodes._

import org.apache.spark.internal.Logging


private[spark] object LambdaClosureCleaner extends Logging {

  private[util] def clean(closure: AnyRef): Unit = {
    val writeReplaceMethod: Method = try {
      closure.getClass.getDeclaredMethod("writeReplace")
    } catch {
      case e: java.lang.NoSuchMethodException =>
        logWarning("Expected a Java lambda; got " + closure.getClass.getName)
        return
    }

    writeReplaceMethod.setAccessible(true)
    // Because we still need to support Java 7, we must use reflection here.
    val serializedLambda: AnyRef = writeReplaceMethod.invoke(closure)
    if (serializedLambda.getClass.getName != "java.lang.invoke.SerializedLambda") {
      logWarning("Closure's writeReplace() method " +
        s"returned ${serializedLambda.getClass.getName}, not SerializedLambda")
      return
    }

    val serializedLambdaClass = Utils.classForName("java.lang.invoke.SerializedLambda")

    val implClassName = serializedLambdaClass
      .getDeclaredMethod("getImplClass").invoke(serializedLambda).asInstanceOf[String]
    // TODO: we do not want to unconditionally strip this suffix.
    val implMethodName = {
      serializedLambdaClass
        .getDeclaredMethod("getImplMethodName").invoke(serializedLambda).asInstanceOf[String]
        .stripSuffix("$adapted")
    }
    val implMethodSignature = serializedLambdaClass
      .getDeclaredMethod("getImplMethodSignature").invoke(serializedLambda).asInstanceOf[String]
    val capturedArgCount = serializedLambdaClass
      .getDeclaredMethod("getCapturedArgCount").invoke(serializedLambda).asInstanceOf[Int]
    val capturedArgs = (0 until capturedArgCount).map { argNum: Int =>
      serializedLambdaClass
        .getDeclaredMethod("getCapturedArg", java.lang.Integer.TYPE)
        .invoke(serializedLambda, argNum.asInstanceOf[Object])
    }.toSeq
    assert(capturedArgs.size == capturedArgCount)
    val implClass = Utils.classForName(implClassName.replaceAllLiterally("/", "."))

    // Fail fast if we detect return statements in closures.
    // TODO: match the impl method based on its type signature as well, not just its name.
    ClosureCleaner
      .getClassReader(implClass)
      .accept(new LambdaReturnStatementFinder(implMethodName), 0)

    // Check serializable TODO: add flag
    ClosureCleaner.ensureSerializable(closure)
    capturedArgs.foreach(ClosureCleaner.clean(_))

    // TODO: null fields to render the closure serializable?
  }
}


private class LambdaReturnStatementFinder(targetMethodName: String) extends ClassVisitor(ASM5) {
  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]): MethodVisitor = {
    if (name == targetMethodName) {
      new MethodVisitor(ASM5) {
        override def visitTypeInsn(op: Int, tp: String) {
          if (op == NEW && tp.contains("scala/runtime/NonLocalReturnControl")) {
            throw new ReturnStatementInClosureException
          }
        }
      }
    } else {
      new MethodVisitor(ASM5) {}
    }
  }
}