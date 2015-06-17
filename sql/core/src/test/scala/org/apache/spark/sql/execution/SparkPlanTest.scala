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

package org.apache.spark.sql.execution

import scala.util.control.NonFatal
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.catalyst.util._

/**
 * Base class for writing tests for individual physical operators. For an example of how this class
 * can be used, see [[SortSuite]].
 */
class SparkPlanTest extends SparkFunSuite {

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate the
   *                     physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(
      input: DataFrame,
      planFunction: SparkPlan => SparkPlan,
      expectedAnswer: Seq[Row]): Unit = {
    SparkPlanTest.checkAnswer(input, planFunction, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate the
   *                     physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Product]]s.
   */
  protected def checkAnswer[A <: Product : TypeTag](
      input: Seq[A],
      planFunction: SparkPlan => SparkPlan,
      expectedAnswer: Seq[A]): Unit = {
    val inputDf = TestSQLContext.createDataFrame(input)
    val expectedRows = expectedAnswer.map(t => Row.apply(t))
    SparkPlanTest.checkAnswer(inputDf, planFunction, expectedRows) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }
}

/**
 * Helper methods for writing tests of individual physical operators.
 */
object SparkPlanTest {

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate the
   *                     physical operator that's being tested.
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  def checkAnswer(
      input: DataFrame,
      planFunction: SparkPlan => SparkPlan,
      expectedAnswer: Seq[Row]): Option[String] = {

    val outputPlan = planFunction(input.queryExecution.sparkPlan)

    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
      // equality test.
      // This function is copied from Catalyst's QueryTest
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case b: Array[Byte] => b.toSeq
          case o => o
        })
      }
      converted.sortBy(_.toString())
    }

    val sparkAnswer: Seq[Row] = try {
      outputPlan.executeCollect().toSeq
    } catch {
      case NonFatal(e) =>
        val errorMessage =
          s"""
             | Exception thrown while executing Spark plan:
             | $outputPlan
             | == Exception ==
             | $e
             | ${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      val errorMessage =
        s"""
           | Results do not match for Spark plan:
           | $outputPlan
           | == Results ==
           | ${sideBySide(
              s"== Correct Answer - ${expectedAnswer.size} ==" +:
              prepareAnswer(expectedAnswer).map(_.toString()),
              s"== Spark Answer - ${sparkAnswer.size} ==" +:
              prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")}
      """.stripMargin
      return Some(errorMessage)
    }

    None
  }
}

