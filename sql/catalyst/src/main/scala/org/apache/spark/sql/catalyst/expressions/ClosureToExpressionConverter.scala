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

// scalastyle:off
// TODO: fix

package org.apache.spark.sql.catalyst.expressions

import scala.collection.mutable

import javassist.{Modifier, ClassPool, CtMethod}
import javassist.bytecode.Opcode._
import javassist.bytecode.{ConstPool, InstructionPrinter}
import javassist.bytecode.analysis.Analyzer

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

object ClosureToExpressionConverter {

  val analyzer = new Analyzer()
  val classPool = ClassPool.getDefault

  def analyzeMethod(method: CtMethod, children: Seq[Expression]): Option[Expression] = {
    println ("-" * 80)
    println (method.getName)
    println(children)
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
          case INVOKEVIRTUAL => {
            val mrClassName = constPool.getMethodrefClassName(instructions.u16bitAt(pos + 1))
            val mrMethName = constPool.getMethodrefName(instructions.u16bitAt(pos + 1))
            val mrDesc = constPool.getMethodrefType(instructions.u16bitAt(pos + 1))
            val ctClass = classPool.get(mrClassName)
            val ctMethod = ctClass.getMethod(mrMethName, mrDesc)
            val numParameters = ctMethod.getParameterTypes.length
            numParameters + 1
          }
          case _ => STACK_GROW(op)
        }
      }
      val targetVarName = stack(stackHeight + stackGrow)


      def stackHeadMinus1 = exprs(stack(stackHeight - 1))
      def stackHead = exprs(stack(stackHeight))

      exprs(targetVarName) = op match {
        case ALOAD_0 => children(0)
        case ALOAD_1 => children(1)
        case ALOAD_2 => children(2)
        case ICONST_1 => Literal(1)
        case ICONST_2 => Literal(2)
        case ILOAD_0 => children(0)
        case ILOAD_1 => children(1)
        case ILOAD_2 => children(2)
        case IMUL => Multiply(stackHeadMinus1, stackHead)
        case IADD => Add(stackHeadMinus1, stackHead)
        case LDC =>
          val cp_index = instructions.byteAt(pos + 1)
          val value = constPool.getTag(cp_index) match {
            case ConstPool.CONST_Integer => constPool.getIntegerInfo(cp_index)
          }
          Literal(value)
//          println(s"stack$stackHeight = $value")
        case INVOKEVIRTUAL =>
          val mrClassName = constPool.getMethodrefClassName(instructions.u16bitAt(pos + 1))
          val mrMethName = constPool.getMethodrefName(instructions.u16bitAt(pos + 1))
          val mrDesc = constPool.getMethodrefType(instructions.u16bitAt(pos + 1))
          val ctClass = classPool.get(mrClassName)
          val ctMethod = ctClass.getMethod(mrMethName, mrDesc)
          val numParameters = ctMethod.getParameterTypes.length
          println(s"stack$stackHeight = ($mrClassName stack${stackHeight - numParameters}).$mrMethName(${(1 to numParameters).map(i => s"stack${stackHeight - i}").mkString(", ")})")
          val attributes = (stackHeight - numParameters to stackHeight).map(i => exprs(stack(i)))
          assert(attributes.length == numParameters + 1)
          analyzeMethod(ctMethod, attributes) match {
            case Some(expr) => expr
            case None =>
              println("ERROR: Problem analyzing method call")
              return None
          }
        case IRETURN =>
          return Some(exprs(stack(stackHeight)))
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

  def isStatic(method: CtMethod): Boolean = Modifier.isStatic(method.getModifiers)

  // TODO: handle argument types
  def convert(closure: Object): Option[Expression] = {
    val ctClass = classPool.get(closure.getClass.getName)
    val applyMethods = ctClass.getMethods.filter(_.getName == "apply")
    // Take the first apply() method which can be resolved to an expression
    applyMethods.flatMap { method =>
      println(" \n  " * 10)
      val attributes = (1 to method.getParameterTypes.length).map(i => UnresolvedAttribute(s"attr$i"))
      if (isStatic(method)) {
        analyzeMethod(method, attributes)
      } else {
        analyzeMethod(method, Seq(UnresolvedAttribute("this")) ++ attributes)
      }
    }.headOption
  }

  val x = (y: Int, z: Int) => (y + 1 + z) * 2 + 342424 * y

  def main(args: Array[String]): Unit = {
    println("THE RESULT OF EXPR IS:\n" + convert(x).getOrElse("ERROR!"))
  }
}