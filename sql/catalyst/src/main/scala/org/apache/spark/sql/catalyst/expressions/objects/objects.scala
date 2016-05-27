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

package org.apache.spark.sql.catalyst.expressions.objects

import java.lang.reflect.Modifier

import scala.annotation.tailrec
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.serializer._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

/**
 * Invokes a static function, returning the result.  By default, any of the arguments being null
 * will result in returning null instead of calling the function.
 *
 * @param staticObject The target of the static call.  This can either be the object itself
 *                     (methods defined on scala objects), or the class object
 *                     (static methods defined in java).
 * @param dataType The expected return type of the function call
 * @param functionName The name of the method to call.
 * @param arguments An optional list of expressions to pass as arguments to the function.
 * @param propagateNull When true, and any of the arguments is null, null will be returned instead
 *                      of calling the function.
 */
case class StaticInvoke(
    staticObject: Class[_],
    dataType: DataType,
    functionName: String,
    arguments: Seq[Expression] = Nil,
    propagateNull: Boolean = true) extends Expression with NonSQLExpression {

  val objectName = staticObject.getName.stripSuffix("$")

  override def nullable: Boolean = true
  override def children: Seq[Expression] = arguments

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val argGen = arguments.map(_.genCode(ctx))
    val argString = argGen.map(_.value).mkString(", ")

    val callFunc = s"$objectName.$functionName($argString)"

    val setIsNull = if (propagateNull && arguments.nonEmpty) {
      s"boolean ${ev.isNull} = ${argGen.map(_.isNull).mkString(" || ")};"
    } else {
      s"boolean ${ev.isNull} = false;"
    }

    // If the function can return null, we do an extra check to make sure our null bit is still set
    // correctly.
    val postNullCheck = if (ctx.defaultValue(dataType) == "null") {
      s"${ev.isNull} = ${ev.value} == null;"
    } else {
      ""
    }

    val code = s"""
      ${argGen.map(_.code).mkString("\n")}
      $setIsNull
      final $javaType ${ev.value} = ${ev.isNull} ? ${ctx.defaultValue(dataType)} : $callFunc;
      $postNullCheck
     """
    ev.copy(code = code)
  }
}

/**
 * Calls the specified function on an object, optionally passing arguments.  If the `targetObject`
 * expression evaluates to null then null will be returned.
 *
 * In some cases, due to erasure, the schema may expect a primitive type when in fact the method
 * is returning java.lang.Object.  In this case, we will generate code that attempts to unbox the
 * value automatically.
 *
 * @param targetObject An expression that will return the object to call the method on.
 * @param functionName The name of the method to call.
 * @param dataType The expected return type of the function.
 * @param arguments An optional list of expressions, whos evaluation will be passed to the function.
 */
case class Invoke(
    targetObject: Expression,
    functionName: String,
    dataType: DataType,
    arguments: Seq[Expression] = Nil,
    propagateNull: Boolean = true) extends Expression with NonSQLExpression {

  override def nullable: Boolean = true
  override def children: Seq[Expression] = targetObject +: arguments

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  @transient lazy val method = targetObject.dataType match {
    case ObjectType(cls) =>
      val m = cls.getMethods.find(_.getName == functionName)
      if (m.isEmpty) {
        sys.error(s"Couldn't find $functionName on $cls")
      } else {
        m
      }
    case _ => None
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val obj = targetObject.genCode(ctx)
    val argGen = arguments.map(_.genCode(ctx))
    val argString = argGen.map(_.value).mkString(", ")

    val callFunc = if (method.isDefined && method.get.getReturnType.isPrimitive) {
      s"${obj.value}.$functionName($argString)"
    } else {
      s"(${ctx.boxedType(javaType)}) ${obj.value}.$functionName($argString)"
    }

    val setIsNull = if (propagateNull && arguments.nonEmpty) {
      s"boolean ${ev.isNull} = ${obj.isNull} || ${argGen.map(_.isNull).mkString(" || ")};"
    } else {
      s"boolean ${ev.isNull} = ${obj.isNull};"
    }

    val evaluate = if (method.forall(_.getExceptionTypes.isEmpty)) {
      s"final $javaType ${ev.value} = ${ev.isNull} ? ${ctx.defaultValue(dataType)} : $callFunc;"
    } else {
      s"""
        $javaType ${ev.value} = ${ctx.defaultValue(javaType)};
        try {
          ${ev.value} = ${ev.isNull} ? ${ctx.defaultValue(javaType)} : $callFunc;
        } catch (Exception e) {
          org.apache.spark.unsafe.Platform.throwException(e);
        }
      """
    }

    // If the function can return null, we do an extra check to make sure our null bit is still set
    // correctly.
    val postNullCheck = if (ctx.defaultValue(dataType) == "null") {
      s"${ev.isNull} = ${ev.value} == null;"
    } else {
      ""
    }

    val code = s"""
      ${obj.code}
      ${argGen.map(_.code).mkString("\n")}
      $setIsNull
      $evaluate
      $postNullCheck
     """
    ev.copy(code = code)
  }

  override def toString: String = s"$targetObject.$functionName"
}

object NewInstance {
  def apply(
      cls: Class[_],
      arguments: Seq[Expression],
      dataType: DataType,
      propagateNull: Boolean = true): NewInstance =
    new NewInstance(cls, arguments, propagateNull, dataType, None)
}

/**
 * Constructs a new instance of the given class, using the result of evaluating the specified
 * expressions as arguments.
 *
 * @param cls The class to construct.
 * @param arguments A list of expression to use as arguments to the constructor.
 * @param propagateNull When true, if any of the arguments is null, then null will be returned
 *                      instead of trying to construct the object.
 * @param dataType The type of object being constructed, as a Spark SQL datatype.  This allows you
 *                 to manually specify the type when the object in question is a valid internal
 *                 representation (i.e. ArrayData) instead of an object.
 * @param outerPointer If the object being constructed is an inner class, the outerPointer for the
 *                     containing class must be specified. This parameter is defined as an optional
 *                     function, which allows us to get the outer pointer lazily,and it's useful if
 *                     the inner class is defined in REPL.
 */
case class NewInstance(
    cls: Class[_],
    arguments: Seq[Expression],
    propagateNull: Boolean,
    dataType: DataType,
    outerPointer: Option[() => AnyRef]) extends Expression with NonSQLExpression {
  private val className = cls.getName

  override def nullable: Boolean = propagateNull

  override def children: Seq[Expression] = arguments

  override lazy val resolved: Boolean = {
    // If the class to construct is an inner class, we need to get its outer pointer, or this
    // expression should be regarded as unresolved.
    // Note that static inner classes (e.g., inner classes within Scala objects) don't need
    // outer pointer registration.
    val needOuterPointer =
      outerPointer.isEmpty && cls.isMemberClass && !Modifier.isStatic(cls.getModifiers)
    childrenResolved && !needOuterPointer
  }

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val argGen = arguments.map(_.genCode(ctx))
    val argString = argGen.map(_.value).mkString(", ")

    val outer = outerPointer.map(func => Literal.fromObject(func()).genCode(ctx))

    var isNull = ev.isNull
    val setIsNull = if (propagateNull && arguments.nonEmpty) {
      s"final boolean $isNull = ${argGen.map(_.isNull).mkString(" || ")};"
    } else {
      isNull = "false"
      ""
    }

    val constructorCall = outer.map { gen =>
      s"""${gen.value}.new ${cls.getSimpleName}($argString)"""
    }.getOrElse {
      s"new $className($argString)"
    }

    val code = s"""
      ${argGen.map(_.code).mkString("\n")}
      ${outer.map(_.code).getOrElse("")}
      $setIsNull
      final $javaType ${ev.value} = $isNull ? ${ctx.defaultValue(javaType)} : $constructorCall;
     """
    ev.copy(code = code, isNull = isNull)
  }

  override def toString: String = s"newInstance($cls)"
}

/**
 * Given an expression that returns on object of type `Option[_]`, this expression unwraps the
 * option into the specified Spark SQL datatype.  In the case of `None`, the nullbit is set instead.
 *
 * @param dataType The expected unwrapped option type.
 * @param child An expression that returns an `Option`
 */
case class UnwrapOption(
    dataType: DataType,
    child: Expression) extends UnaryExpression with NonSQLExpression with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = ObjectType :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val inputObject = child.genCode(ctx)

    val code = s"""
      ${inputObject.code}

      final boolean ${ev.isNull} = ${inputObject.isNull} || ${inputObject.value}.isEmpty();
      $javaType ${ev.value} = ${ev.isNull} ?
        ${ctx.defaultValue(javaType)} : (${ctx.boxedType(javaType)}) ${inputObject.value}.get();
    """
    ev.copy(code = code)
  }
}

/**
 * Converts the result of evaluating `child` into an option, checking both the isNull bit and
 * (in the case of reference types) equality with null.
 *
 * @param child The expression to evaluate and wrap.
 * @param optType The type of this option.
 */
case class WrapOption(child: Expression, optType: DataType)
  extends UnaryExpression with NonSQLExpression with ExpectsInputTypes {

  override def dataType: DataType = ObjectType(classOf[Option[_]])

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = optType :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val inputObject = child.genCode(ctx)

    val code = s"""
      ${inputObject.code}

      scala.Option ${ev.value} =
        ${inputObject.isNull} ?
        scala.Option$$.MODULE$$.apply(null) : new scala.Some(${inputObject.value});
    """
    ev.copy(code = code, isNull = "false")
  }
}

/**
 * A place holder for the loop variable used in [[MapObjects]].  This should never be constructed
 * manually, but will instead be passed into the provided lambda function.
 */
case class LambdaVariable(value: String, isNull: String, dataType: DataType) extends LeafExpression
  with Unevaluable with NonSQLExpression {

  override def nullable: Boolean = true

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(code = "", value = value, isNull = isNull)
  }
}

object MapObjects {
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  def apply(
      function: Expression => Expression,
      inputData: Expression,
      elementType: DataType): MapObjects = {
    val loopValue = "MapObjects_loopValue" + curId.getAndIncrement()
    val loopIsNull = "MapObjects_loopIsNull" + curId.getAndIncrement()
    val loopVar = LambdaVariable(loopValue, loopIsNull, elementType)
    MapObjects(loopVar, function(loopVar), inputData)
  }
}

/**
 * Applies the given expression to every element of a collection of items, returning the result
 * as an ArrayType.  This is similar to a typical map operation, but where the lambda function
 * is expressed using catalyst expressions.
 *
 * The following collection ObjectTypes are currently supported:
 *   Seq, Array, ArrayData, java.util.List
 *
 * @param loopVar A place holder that used as the loop variable when iterate the collection, and
 *                used as input for the `lambdaFunction`. It also carries the element type info.
 * @param lambdaFunction A function that take the `loopVar` as input, and used as lambda function
 *                       to handle collection elements.
 * @param inputData An expression that when evaluated returns a collection object.
 */
case class MapObjects private(
    loopVar: LambdaVariable,
    lambdaFunction: Expression,
    inputData: Expression) extends Expression with NonSQLExpression {

  override def nullable: Boolean = true

  override def children: Seq[Expression] = lambdaFunction :: inputData :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def dataType: DataType = ArrayType(lambdaFunction.dataType)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val elementJavaType = ctx.javaType(loopVar.dataType)
    ctx.addMutableState("boolean", loopVar.isNull, "")
    ctx.addMutableState(elementJavaType, loopVar.value, "")
    val genInputData = inputData.genCode(ctx)
    val genFunction = lambdaFunction.genCode(ctx)
    val dataLength = ctx.freshName("dataLength")
    val convertedArray = ctx.freshName("convertedArray")
    val loopIndex = ctx.freshName("loopIndex")

    val convertedType = ctx.boxedType(lambdaFunction.dataType)

    // Because of the way Java defines nested arrays, we have to handle the syntax specially.
    // Specifically, we have to insert the [$dataLength] in between the type and any extra nested
    // array declarations (i.e. new String[1][]).
    val arrayConstructor = if (convertedType contains "[]") {
      val rawType = convertedType.takeWhile(_ != '[')
      val arrayPart = convertedType.reverse.takeWhile(c => c == '[' || c == ']').reverse
      s"new $rawType[$dataLength]$arrayPart"
    } else {
      s"new $convertedType[$dataLength]"
    }

    // In RowEncoder, we use `Object` to represent Array or Seq, so we need to determine the type
    // of input collection at runtime for this case.
    val seq = ctx.freshName("seq")
    val array = ctx.freshName("array")
    val determineCollectionType = inputData.dataType match {
      case ObjectType(cls) if cls == classOf[Object] =>
        val seqClass = classOf[Seq[_]].getName
        s"""
          $seqClass $seq = null;
          $elementJavaType[] $array = null;
          if (${genInputData.value}.getClass().isArray()) {
            $array = ($elementJavaType[]) ${genInputData.value};
          } else {
            $seq = ($seqClass) ${genInputData.value};
          }
         """
      case _ => ""
    }


    val (getLength, getLoopVar) = inputData.dataType match {
      case ObjectType(cls) if classOf[Seq[_]].isAssignableFrom(cls) =>
        s"${genInputData.value}.size()" -> s"${genInputData.value}.apply($loopIndex)"
      case ObjectType(cls) if cls.isArray =>
        s"${genInputData.value}.length" -> s"${genInputData.value}[$loopIndex]"
      case ObjectType(cls) if classOf[java.util.List[_]].isAssignableFrom(cls) =>
        s"${genInputData.value}.size()" -> s"${genInputData.value}.get($loopIndex)"
      case ArrayType(et, _) =>
        s"${genInputData.value}.numElements()" -> ctx.getValue(genInputData.value, et, loopIndex)
      case ObjectType(cls) if cls == classOf[Object] =>
        s"$seq == null ? $array.length : $seq.size()" ->
          s"$seq == null ? $array[$loopIndex] : $seq.apply($loopIndex)"
    }

    val loopNullCheck = inputData.dataType match {
      case _: ArrayType => s"${loopVar.isNull} = ${genInputData.value}.isNullAt($loopIndex);"
      // The element of primitive array will never be null.
      case ObjectType(cls) if cls.isArray && cls.getComponentType.isPrimitive =>
        s"${loopVar.isNull} = false"
      case _ => s"${loopVar.isNull} = ${loopVar.value} == null;"
    }

    val code = s"""
      ${genInputData.code}
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};

      if (!${genInputData.isNull}) {
        $determineCollectionType
        $convertedType[] $convertedArray = null;
        int $dataLength = $getLength;
        $convertedArray = $arrayConstructor;

        int $loopIndex = 0;
        while ($loopIndex < $dataLength) {
          ${loopVar.value} = ($elementJavaType) ($getLoopVar);
          $loopNullCheck

          ${genFunction.code}
          if (${genFunction.isNull}) {
            $convertedArray[$loopIndex] = null;
          } else {
            $convertedArray[$loopIndex] = ${genFunction.value};
          }

          $loopIndex += 1;
        }

        ${ev.value} = new ${classOf[GenericArrayData].getName}($convertedArray);
      }
    """
    ev.copy(code = code, isNull = genInputData.isNull)
  }
}

/**
 * Constructs a new external row, using the result of evaluating the specified expressions
 * as content.
 *
 * @param children A list of expression to use as content of the external row.
 */
case class CreateExternalRow(children: Seq[Expression], schema: StructType)
  extends Expression with NonSQLExpression {

  override def dataType: DataType = ObjectType(classOf[Row])

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rowClass = classOf[GenericRowWithSchema].getName
    val values = ctx.freshName("values")
    ctx.addMutableState("Object[]", values, "")

    val childrenCodes = children.zipWithIndex.map { case (e, i) =>
      val eval = e.genCode(ctx)
      eval.code + s"""
          if (${eval.isNull}) {
            $values[$i] = null;
          } else {
            $values[$i] = ${eval.value};
          }
         """
    }

    val childrenCode = ctx.splitExpressions(ctx.INPUT_ROW, childrenCodes)
    val schemaField = ctx.addReferenceObj("schema", schema)

    val code = s"""
      $values = new Object[${children.size}];
      $childrenCode
      final ${classOf[Row].getName} ${ev.value} = new $rowClass($values, this.$schemaField);
      """
    ev.copy(code = code, isNull = "false")
  }
}

/**
 * Serializes an input object using a generic serializer (Kryo or Java).
 *
 * @param kryo if true, use Kryo. Otherwise, use Java.
 */
case class EncodeUsingSerializer(child: Expression, kryo: Boolean)
  extends UnaryExpression with NonSQLExpression {

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Code to initialize the serializer.
    val serializer = ctx.freshName("serializer")
    val (serializerClass, serializerInstanceClass) = {
      if (kryo) {
        (classOf[KryoSerializer].getName, classOf[KryoSerializerInstance].getName)
      } else {
        (classOf[JavaSerializer].getName, classOf[JavaSerializerInstance].getName)
      }
    }
    val sparkConf = s"new ${classOf[SparkConf].getName}()"
    ctx.addMutableState(
      serializerInstanceClass,
      serializer,
      s"$serializer = ($serializerInstanceClass) new $serializerClass($sparkConf).newInstance();")

    // Code to serialize.
    val input = child.genCode(ctx)
    val javaType = ctx.javaType(dataType)
    val serialize = s"$serializer.serialize(${input.value}, null).array()"

    val code = s"""
      ${input.code}
      final $javaType ${ev.value} = ${input.isNull} ? ${ctx.defaultValue(javaType)} : $serialize;
     """
    ev.copy(code = code, isNull = input.isNull)
  }

  override def dataType: DataType = BinaryType
}

/**
 * Serializes an input object using a generic serializer (Kryo or Java).  Note that the ClassTag
 * is not an implicit parameter because TreeNode cannot copy implicit parameters.
 *
 * @param kryo if true, use Kryo. Otherwise, use Java.
 */
case class DecodeUsingSerializer[T](child: Expression, tag: ClassTag[T], kryo: Boolean)
  extends UnaryExpression with NonSQLExpression {

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Code to initialize the serializer.
    val serializer = ctx.freshName("serializer")
    val (serializerClass, serializerInstanceClass) = {
      if (kryo) {
        (classOf[KryoSerializer].getName, classOf[KryoSerializerInstance].getName)
      } else {
        (classOf[JavaSerializer].getName, classOf[JavaSerializerInstance].getName)
      }
    }
    val sparkConf = s"new ${classOf[SparkConf].getName}()"
    ctx.addMutableState(
      serializerInstanceClass,
      serializer,
      s"$serializer = ($serializerInstanceClass) new $serializerClass($sparkConf).newInstance();")

    // Code to deserialize.
    val input = child.genCode(ctx)
    val javaType = ctx.javaType(dataType)
    val deserialize =
      s"($javaType) $serializer.deserialize(java.nio.ByteBuffer.wrap(${input.value}), null)"

    val code = s"""
      ${input.code}
      final $javaType ${ev.value} = ${input.isNull} ? ${ctx.defaultValue(javaType)} : $deserialize;
     """
    ev.copy(code = code, isNull = input.isNull)
  }

  override def dataType: DataType = ObjectType(tag.runtimeClass)
}

/**
 * Initialize a Java Bean instance by setting its field values via setters.
 */
case class InitializeJavaBean(beanInstance: Expression, setters: Map[String, Expression])
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = beanInstance.nullable
  override def children: Seq[Expression] = beanInstance +: setters.values.toSeq
  override def dataType: DataType = beanInstance.dataType

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val instanceGen = beanInstance.genCode(ctx)

    val initialize = setters.map {
      case (setterMethod, fieldValue) =>
        val fieldGen = fieldValue.genCode(ctx)
        s"""
           ${fieldGen.code}
           ${instanceGen.value}.$setterMethod(${fieldGen.value});
         """
    }

    val code = s"""
      ${instanceGen.code}
      if (!${instanceGen.isNull}) {
        ${initialize.mkString("\n")}
      }
     """
    ev.copy(code = code, isNull = instanceGen.isNull, value = instanceGen.value)
  }
}

/**
 * Asserts that input values of a non-nullable child expression are not null.
 *
 * Note that there are cases where `child.nullable == true`, while we still needs to add this
 * assertion.  Consider a nullable column `s` whose data type is a struct containing a non-nullable
 * `Int` field named `i`.  Expression `s.i` is nullable because `s` can be null.  However, for all
 * non-null `s`, `s.i` can't be null.
 */
case class AssertNotNull(child: Expression, walkedTypePath: Seq[String])
  extends UnaryExpression with NonSQLExpression {

  override def dataType: DataType = child.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)

    val errMsg = "Null value appeared in non-nullable field:" +
      walkedTypePath.mkString("\n", "\n", "\n") +
      "If the schema is inferred from a Scala tuple/case class, or a Java bean, " +
      "please try to use scala.Option[_] or other nullable types " +
      "(e.g. java.lang.Integer instead of int/scala.Int)."
    val errMsgField = ctx.addReferenceObj("errMsg", errMsg)

    val code = s"""
      ${childGen.code}

      if (${childGen.isNull}) {
        throw new RuntimeException(this.$errMsgField);
      }
     """
    ev.copy(code = code, isNull = "false", value = childGen.value)
  }
}

/**
 * Returns the value of field at index `index` from the external row `child`.
 * This class can be viewed as [[GetStructField]] for [[Row]]s instead of [[InternalRow]]s.
 *
 * Note that the input row and the field we try to get are both guaranteed to be not null, if they
 * are null, a runtime exception will be thrown.
 */
case class GetExternalRowField(
    child: Expression,
    index: Int,
    fieldName: String,
    dataType: DataType) extends UnaryExpression with NonSQLExpression {

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val row = child.genCode(ctx)

    val getField = dataType match {
      case ObjectType(x) if x == classOf[Row] => s"""${row.value}.getStruct($index)"""
      case _ => s"""(${ctx.boxedType(dataType)}) ${row.value}.get($index)"""
    }

    val code = s"""
      ${row.code}

      if (${row.isNull}) {
        throw new RuntimeException("The input external row cannot be null.");
      }

      if (${row.value}.isNullAt($index)) {
        throw new RuntimeException("The ${index}th field '$fieldName' of input row " +
          "cannot be null.");
      }

      final ${ctx.javaType(dataType)} ${ev.value} = $getField;
     """
    ev.copy(code = code, isNull = "false")
  }
}
