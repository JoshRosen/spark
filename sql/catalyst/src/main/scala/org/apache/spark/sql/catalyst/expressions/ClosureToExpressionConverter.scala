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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{ExprCode, CodegenContext}
import org.apache.spark.sql.types._

import scala.collection.mutable

import javassist.{Modifier, ClassPool, CtMethod}
import javassist.bytecode.Opcode._
import javassist.bytecode.{ConstPool, InstructionPrinter}
import javassist.bytecode.analysis.Analyzer

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

object ClosureToExpressionConverter {

  val analyzer = new Analyzer()
  val classPool = ClassPool.getDefault

  def analyzeMethod(
      method: CtMethod,
      schema: StructType,
      children: Seq[Expression],
      initExprs: Map[String, Expression] = Map.empty,
      stackHeightAtEntry: Int = 0,
      pos: Int = 0): Option[Expression] = {
    println("-" * 80)
    println(method.getName)
    println(children)
    println("-" * 80)

    val exprs = mutable.Map[String, Expression]()
    exprs ++= initExprs
    def stack(i: Int) = s"stack$i"



    val instructions = method.getMethodInfo().getCodeAttribute.iterator()
    instructions.move(pos)
    val constPool = method.getMethodInfo.getConstPool

    {
      val i = method.getMethodInfo().getCodeAttribute.iterator()
      while (i.hasNext) {
        println(InstructionPrinter.instructionString(i, i.next(), constPool))
      }
    }


    def ldc(pos: Int): Any = _ldc(pos, instructions.byteAt(pos + 1))
    def ldcw(pos: Int): Any = _ldc(pos, instructions.u16bitAt(pos + 1))
    def _ldc(pos: Int, cp_index: Int): Any = {
      constPool.getTag(cp_index) match {
        case ConstPool.CONST_Double => constPool.getDoubleInfo(cp_index)
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

    var stackHeight = stackHeightAtEntry

    while (instructions.hasNext) {
      val pos = instructions.next()
      val op = instructions.byteAt(pos)
      val mnemonic = InstructionPrinter.instructionString(instructions, pos, constPool)
      println("*" * 20 + " " + mnemonic + s" (stack = $stackHeight) " + "*" * 20)

      val stackGrow: Int = {
        op match {
          case INVOKEVIRTUAL => -1 * getInvokeVirtualTarget(pos).getParameterTypes.length
          case INVOKEINTERFACE => -1 * getInvokeInterfaceTarget(pos).getParameterTypes.length
          case LDC2_W => 1 // TODO: in reality, this pushes 2; this is a hack.
          case LCONST_0 | LCONST_1 => 1 // hack
          case LADD | LMUL => -1
          case I2L | I2D => 0 // hack
          case _ => STACK_GROW(op)
        }
      }
      val targetVarName = stack(stackHeight + stackGrow)


      def stackHeadMinus1 = exprs(stack(stackHeight - 1))
      def stackHead = exprs(stack(stackHeight))

      def analyzeCMP(
          compOp: (Expression, Expression) => Predicate): Option[Expression] = {
        val trueJumpTarget = instructions.s16bitAt(pos + 1) + pos
        val trueExpression = analyzeMethod(
          method,
          schema,
          children,
          exprs.toMap,
          stackHeight + stackGrow,
          trueJumpTarget)
        val falseExpression = analyzeMethod(
          method,
          schema,
          children,
          exprs.toMap,
          stackHeight + stackGrow,
          instructions.next())
        if (trueExpression.isDefined && falseExpression.isDefined) {
          Some(If(compOp(stackHeadMinus1, stackHead), trueExpression.get, falseExpression.get))
        } else {
          None
        }
      }

      exprs(targetVarName) = op match {
        case ALOAD_0 => children(0)
        case ALOAD_1 => children(1)
        case ALOAD_2 => children(2)
        case BIPUSH => Literal(instructions.byteAt(pos + 1)) // TODO: byte must be sign-extended into an integer value?
        case ICONST_0 => Literal(0)
        case ICONST_1 => Literal(1)
        case ICONST_2 => Literal(2)
        case ICONST_3 => Literal(3)
        case ICONST_4 => Literal(4)
        case LCONST_0 => Literal(0L)
        case ILOAD_0 => children(0)
        case ILOAD_1 => children(1)
        case ILOAD_2 => children(2)
        case GOTO =>
          val target = instructions.s16bitAt(pos + 1) + pos
          return analyzeMethod(method, schema, children, exprs.toMap, stackHeight, target)
        case I2D => Cast(stackHead, DoubleType)
        case I2L => Cast(stackHead, LongType)
        case IMUL | LMUL => Multiply(stackHeadMinus1, stackHead)
        case IADD | DADD | LADD => Add(stackHeadMinus1, stackHead)
        case LDC => Literal(ldc(pos))
        case LDC2_W => Literal(ldcw(pos)) // Pushes two words onto the stack, but we're going to only push one
        case IF_ICMPLE => return analyzeCMP(LessThanOrEqual)
        case IF_ICMPNE => return analyzeCMP((e1, e2) => Not(EqualTo(e1, e2)))
        case INVOKEINTERFACE =>
          val target = getInvokeInterfaceTarget(pos)
          val getters = Set("getInt", "getLong")
          if (target.getDeclaringClass.getName == classOf[Row].getName) {
            if (getters.contains(target.getName)) {
              val fieldNumber = stackHead.asInstanceOf[Literal].value.asInstanceOf[Int]
              NPEOnNull(UnresolvedAttribute(schema.fields(fieldNumber).name))
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
          analyzeMethod(target, schema, attributes) match {
            case Some(expr) => expr
            case None =>
              println("ERROR: Problem analyzing method call")
              return None
          }
        case DRETURN | IRETURN | LRETURN =>
          return Some(exprs(stack(stackHeight)))
        case _ =>
          println(s"ERROR: Unknown opcode $mnemonic")
          return None
      }
      stackHeight += stackGrow

      exprs.toSeq.sortBy(_._1).foreach { case (label, value) =>
        println(s"    $label = $value")
      }
    }
    throw new Exception("oh no!")
  }

  def isStatic(method: CtMethod): Boolean = Modifier.isStatic(method.getModifiers)

  // TODO: handle argument types
  // For now, this assumes f: Row => Expr
  def convert(closure: Object, schema: StructType): Option[Expression] = {
    val ctClass = classPool.get(closure.getClass.getName)
    val applyMethods = ctClass.getMethods.filter(_.getName == "apply")
    // Take the first apply() method which can be resolved to an expression
    applyMethods.flatMap { method =>
      println(" \n  " * 10)
      assert(method.getParameterTypes.length == 1)
      val attributes = Seq(UnresolvedAttribute("inputRow"))
      if (isStatic(method)) {
        analyzeMethod(method, schema, attributes)
      } else {
        analyzeMethod(method, schema, Seq(UnresolvedAttribute("this")) ++ attributes)
      }
    }.headOption
  }

  def convertFilter(closure: Object, schema: StructType): Option[Expression] = {
    convert(closure, schema).map { expr => Cast(expr, BooleanType) }
  }

}

/**
 * An expression that throws NullPointerException on null input values.
 */
case class NPEOnNull(child: Expression) extends UnaryExpression with NonSQLExpression {
  override def nullable: Boolean = false

  override def dataType: DataType = child.dataType

  override def eval(input: InternalRow): Any = {
    val result = child.eval(input)
    if (result == null) throw new NullPointerException
    result
  }

  override def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val eval = child.gen(ctx)
    s"""
      ${eval.code}
      if(${eval.isNull}) { throw new NullPointerException(); }
      ${eval.value}
      """
  }
}
