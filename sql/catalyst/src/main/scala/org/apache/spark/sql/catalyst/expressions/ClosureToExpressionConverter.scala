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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, IntegerType}

import scala.collection.mutable

import javassist.{Modifier, ClassPool, CtMethod}
import javassist.bytecode.Opcode._
import javassist.bytecode.{ConstPool, InstructionPrinter}
import javassist.bytecode.analysis.Analyzer

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

object ClosureToExpressionConverter {

  val analyzer = new Analyzer()
  val classPool = ClassPool.getDefault

  // TODO: multiple stack slots for long / double values

  def analyzeMethod(method: CtMethod, children: Seq[Expression]): Option[Expression] = {
    println ("-" * 80)
    println (method.getName)
    println(children)
    println ("-" * 80)

    val exprs = mutable.Map[String, Expression]()
    def stack(i: Int) = s"stack$i"


    val instructions = method.getMethodInfo().getCodeAttribute.iterator()
    val constPool = method.getMethodInfo.getConstPool

    def ldc(pos: Int): Any = _ldc(pos, instructions.byteAt(pos + 1))
    def ldcw(pos: Int): Any = _ldc(pos, instructions.u16bitAt(pos + 1))
    def _ldc(pos: Int, cp_index: Int): Any = {
      constPool.getTag(cp_index) match {
        case ConstPool.CONST_Integer => constPool.getIntegerInfo(cp_index)
        case ConstPool.CONST_Long => constPool.getLongInfo(cp_index)
      }
    }

    def getInvokeVirtualTarget(pos: Int) = {
      val mrClassName = constPool.getMethodrefClassName(instructions.u16bitAt(pos + 1))
      val mrMethName = constPool.getMethodrefName(instructions.u16bitAt(pos + 1))
      val mrDesc = constPool.getMethodrefType(instructions.u16bitAt(pos + 1))
      val ctClass = classPool.get(mrClassName)
      ctClass.getMethod(mrMethName, mrDesc)
    }

    def getInvokeInterfaceTarget(pos: Int) = {
      val mrClassName = constPool.getInterfaceMethodrefClassName(instructions.u16bitAt(pos + 1))
      val mrMethName = constPool.getInterfaceMethodrefName(instructions.u16bitAt(pos + 1))
      val mrDesc = constPool.getInterfaceMethodrefType(instructions.u16bitAt(pos + 1))
      val ctClass = classPool.get(mrClassName)
      ctClass.getMethod(mrMethName, mrDesc)
    }

    var stackHeight = 0

    while (instructions.hasNext) {
      val pos = instructions.next()
      val op = instructions.byteAt(pos)
      val mnemonic = InstructionPrinter.instructionString(instructions, pos, constPool)
      println("*" * 20 + " " + mnemonic + s" (stack = $stackHeight) " + "*" * 20)

      val stackGrow: Int = {
        op match {
          case INVOKEVIRTUAL => getInvokeVirtualTarget(pos).getParameterTypes.length + 1
          case LDC2_W => 1 // TODO: in reality, this pushes 2; this is a hack.
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
        case ICONST_0 => Literal(0)
        case ICONST_1 => Literal(1)
        case ICONST_2 => Literal(2)
        case ILOAD_0 => children(0)
        case ILOAD_1 => children(1)
        case ILOAD_2 => children(2)
        case I2L => Cast(stackHead, LongType)
        case IMUL => Multiply(stackHeadMinus1, stackHead)
        case IADD | LADD => Add(stackHeadMinus1, stackHead)
        case LDC => Literal(ldc(pos))
        case LDC2_W => Literal(ldcw(pos))           // Pushes two words onto the stack, but we're going to only push one
        case INVOKEINTERFACE =>
          val target = getInvokeInterfaceTarget(pos)
          if (target.getDeclaringClass.getName == classOf[Row].getName) {
            if (target.getName == "getInt") {
              GetStructField(stackHeadMinus1, stackHead.asInstanceOf[Literal].value.asInstanceOf[Int])
            } else {
              return None
            }
          } else {
            // TODO: error message
            return None
          }
        case INVOKEVIRTUAL =>
          val target = getInvokeVirtualTarget(pos)
          val numParameters = target.getParameterTypes.length
          val attributes = (stackHeight - numParameters to stackHeight).map(i => exprs(stack(i)))
          assert(attributes.length == numParameters + 1)
          analyzeMethod(target, attributes) match {
            case Some(expr) => expr
            case None =>
              println("ERROR: Problem analyzing method call")
              return None
          }
        case IRETURN | LRETURN =>
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
  // For now, this assumes f: Row => Expr
  def convert(closure: Object): Option[Expression] = {
    val ctClass = classPool.get(closure.getClass.getName)
    val applyMethods = ctClass.getMethods.filter(_.getName == "apply")
    // Take the first apply() method which can be resolved to an expression
    applyMethods.flatMap { method =>
      println(" \n  " * 10)
      assert(method.getParameterTypes.length == 1)
      val attributes = Seq(UnresolvedAttribute("inputRow"))
        if (isStatic(method)) {
        analyzeMethod(method, attributes)
      } else {
        analyzeMethod(method, Seq(UnresolvedAttribute("this")) ++ attributes)
      }
    }.headOption
  }

  val x = (row: Row) => (row.getInt(0) + 1L * 2)

  def main(args: Array[String]): Unit = {
    println("THE RESULT OF EXPR IS:\n" + convert(x).getOrElse("ERROR!"))
  }
}