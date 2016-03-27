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

package org.apache.spark.sql.catalyst.expressions


import java.util.concurrent.atomic.AtomicInteger
import javassist.{CtMethod, CtMember, ClassPool}
import javassist.bytecode.{ConstPool, Opcode, InstructionPrinter}
import javassist.bytecode.analysis.Analyzer

object ClosureToExpressionConverter {

  val analyzer = new Analyzer()
  val classPool = ClassPool.getDefault

  var nextIdentifier = new AtomicInteger(-1)

  def analyzeMethod(method : CtMethod): Unit = {
    println ("-" * 80)
    println (method.getName)
    println ("-" * 80)

    val frames = analyzer.analyze(method)
    val instructions = method.getMethodInfo().getCodeAttribute.iterator()
    val cp = method.getMethodInfo.getConstPool
    var stackHeight = 0
    while (instructions.hasNext) {
      print(s"stack height is $stackHeight | ")
      val pos = instructions.next()
      val op = instructions.byteAt(pos)
      stackHeight += Opcode.STACK_GROW(op)
      val mnemonic = InstructionPrinter.instructionString(instructions, pos, cp)
      op match {
        case Opcode.ALOAD_0 =>
          println(s"stack$stackHeight = local0")
        case Opcode.ILOAD_1 =>
          println(s"stack$stackHeight = local1")
        case Opcode.ICONST_1 =>
          println(s"stack$stackHeight = 1")
        case Opcode.IADD =>
          println(s"stack$stackHeight = stack${stackHeight - 1} + stack${stackHeight}")
        case Opcode.LDC =>
          val cp_index = instructions.byteAt(pos + 1)
          val value = cp.getTag(cp_index) match {
            case ConstPool.CONST_Integer => cp.getIntegerInfo(cp_index)
          }
          println(s"stack$stackHeight = $value")
        case Opcode.INVOKEVIRTUAL =>
          val mrClassName = cp.getMethodrefClassName(instructions.u16bitAt(pos + 1))
          val mrMethName = cp.getMethodrefName(instructions.u16bitAt(pos + 1))
          val mrDesc = cp.getMethodrefType(instructions.u16bitAt(pos + 1))
          val ctClass = classPool.get(mrClassName)
          val ctMethod = ctClass.getMethod(mrMethName, mrDesc)
          val numParameters = ctMethod.getParameterTypes.length
          println(s"stack$stackHeight = ($mrClassName stack${stackHeight - numParameters}).$mrMethName(${(1 to numParameters).map(i => s"stack${stackHeight - i}").mkString(", ")})")
          analyzeMethod(ctMethod)
          stackHeight -= numParameters + 1
        case Opcode.IRETURN =>
          println(s"Return stack${stackHeight + 1}")
        case _ =>
          println(s"Unknown opcode $mnemonic")
      }
//      if (op == Opcode.INVOKEVIRTUAL) {

//        println(InstructionPrinter.instructionString(instructions, pos, cp))
//      } else {
//        println(InstructionPrinter.instructionString(instructions, pos, cp))
//      }
    }
    println
  }

  def convert[T, R](closure: Function[T, R]): Option[Expression] = {

    val ctClass = classPool.get(closure.getClass.getName)
    val applyMethods = ctClass.getMethods.filter(_.getName == "apply")
    applyMethods.foreach { method =>
      analyzeMethod(method)
    }
    None
  }

  val x = (y: Int) => y + 1 + y + 342424

  def main(args: Array[String]): Unit = {
    print(convert(x))
  }
}