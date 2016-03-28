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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

import scala.collection.mutable

object ClosureToExpressionConverter {

  val analyzer = new Analyzer()
  val classPool = ClassPool.getDefault

  def analyzeMethod(method: CtMethod, children: Seq[Expression]): Option[Expression] = {
    println ("-" * 80)
    println (method.getName)
    println ("-" * 80)

    val exprs = mutable.Map[String, Expression]()
    def stack(i: Int) = s"stack$i"


    val instructions = method.getMethodInfo().getCodeAttribute.iterator()
    val constPool = method.getMethodInfo.getConstPool

    var stackHeight = 0

    while (instructions.hasNext) {
      val pos = instructions.next()
      val op = instructions.byteAt(pos)
      val mnemonic = InstructionPrinter.instructionString(instructions, pos, constPool)
      println("*" * 20 + " " + mnemonic + s" (stack = $stackHeight) " + "*" * 20)

      val stackGrow: Int = {
        op match {
          case Opcode.INVOKEVIRTUAL => {
            val mrClassName = constPool.getMethodrefClassName(instructions.u16bitAt(pos + 1))
            val mrMethName = constPool.getMethodrefName(instructions.u16bitAt(pos + 1))
            val mrDesc = constPool.getMethodrefType(instructions.u16bitAt(pos + 1))
            val ctClass = classPool.get(mrClassName)
            val ctMethod = ctClass.getMethod(mrMethName, mrDesc)
            val numParameters = ctMethod.getParameterTypes.length
            numParameters + 1
          }
          case _ => Opcode.STACK_GROW(op)
        }
      }
      val targetVarName = stack(stackHeight + stackGrow)

      op match {
        case Opcode.ALOAD_0 =>
          exprs(targetVarName) = children(0)
        case Opcode.ALOAD_1  | Opcode.ILOAD_1 =>
          exprs(targetVarName) = children(1)
        case Opcode.ICONST_1 =>
          exprs(targetVarName) = Literal(1)
        case Opcode.ICONST_2 =>
          exprs(targetVarName) = Literal(2)
        case Opcode.IMUL =>
          exprs(targetVarName) = Multiply(exprs(stack(stackHeight - 1)), exprs(stack(stackHeight)))
        case Opcode.IADD =>
          exprs(targetVarName) = Add(exprs(stack(stackHeight - 1)), exprs(stack(stackHeight)))
//          println(s"stack$stackHeight = stack${stackHeight - 1} + stack${stackHeight}")
        case Opcode.LDC =>
          val cp_index = instructions.byteAt(pos + 1)
          val value = constPool.getTag(cp_index) match {
            case ConstPool.CONST_Integer => constPool.getIntegerInfo(cp_index)
          }
          exprs(targetVarName) = Literal(value)
//          println(s"stack$stackHeight = $value")
        case Opcode.INVOKEVIRTUAL =>
          val mrClassName = constPool.getMethodrefClassName(instructions.u16bitAt(pos + 1))
          val mrMethName = constPool.getMethodrefName(instructions.u16bitAt(pos + 1))
          val mrDesc = constPool.getMethodrefType(instructions.u16bitAt(pos + 1))
          val ctClass = classPool.get(mrClassName)
          val ctMethod = ctClass.getMethod(mrMethName, mrDesc)
          val numParameters = ctMethod.getParameterTypes.length
          println(s"stack$stackHeight = ($mrClassName stack${stackHeight - numParameters}).$mrMethName(${(1 to numParameters).map(i => s"stack${stackHeight - i}").mkString(", ")})")
          val _this = UnresolvedAttribute("this")
          val attributes = Seq(_this) ++ (stackHeight - numParameters to stackHeight).map(i => exprs(stack(i)))
          analyzeMethod(ctMethod, attributes) match {
            case Some(expr) => exprs(targetVarName) = expr
            case None =>
              println("ERROR: Problem analyzing method call")
              return None
          }
        case Opcode.IRETURN =>
          return Some(exprs(stack(stackHeight)))
          println(s"Return stack${stackHeight + 1}")
        case _ =>
          println(s"ERROR: Unknown opcode $mnemonic")
          return None
      }
      stackHeight += stackGrow

      exprs.toSeq.sortBy(_._1).foreach { case (label, value) =>
        println(s"    $label = $value")
      }
//      if (op == Opcode.INVOKEVIRTUAL) {

//        println(InstructionPrinter.instructionString(instructions, pos, cp))
//      } else {
//        println(InstructionPrinter.instructionString(instructions, pos, cp))
//      }
    }
    throw new Exception("oh no!")
  }

  def convert[T, R](closure: Function[T, R]): Option[Expression] = {

    val ctClass = classPool.get(closure.getClass.getName)
    val applyMethods = ctClass.getMethods.filter(_.getName == "apply")
    applyMethods.flatMap { method =>
      // TODO: static methods need to be done specially
      val _this = UnresolvedAttribute("this")
      val attributes = (1 to method.getParameterTypes.length).map(i => UnresolvedAttribute(s"attr$i"))
      analyzeMethod(method, Seq(_this) ++ attributes)
    }.headOption
  }

  val x = (y: Int) => (y + 1 + y) * 2 + 342424 * y

  def main(args: Array[String]): Unit = {
    println("THE RESULT OF EXPR IS:\n" + convert(x).getOrElse("ERROR!"))
  }
}