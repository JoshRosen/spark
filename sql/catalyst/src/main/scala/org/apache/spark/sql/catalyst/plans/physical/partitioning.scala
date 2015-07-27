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

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.expressions.{Unevaluable, Expression, SortOrder}
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * Specifies how tuples that share common expressions will be distributed when a query is executed
 * in parallel on many machines.  Distribution can be used to refer to two distinct physical
 * properties:
 *  - Inter-node partitioning of data: In this case the distribution describes how tuples are
 *    partitioned across physical machines in a cluster.  Knowing this property allows some
 *    operators (e.g., Aggregate) to perform partition local operations instead of global ones.
 *  - Intra-partition ordering of data: In this case the distribution describes guarantees made
 *    about how tuples are distributed within a single partition.
 */
sealed trait Distribution

/**
 * Represents a distribution where no promises are made about co-location of data.
 */
case object UnspecifiedDistribution extends Distribution

/**
 * Represents a distribution that only has a single partition and all tuples of the dataset
 * are co-located.
 */
case object AllTuples extends Distribution

/**
 * Represents data where tuples that share the same values for the `clustering`
 * [[Expression Expressions]] will be co-located. Based on the context, this
 * can mean such tuples are either co-located in the same partition or they will be contiguous
 * within a single partition. When `nullSafe` is true,
 * for two null values in two rows evaluated by `clustering`,
 * we consider these two nulls are equal.
 */
case class ClusteredDistribution(
    clustering: Seq[Expression],
    nullSafe: Boolean) extends Distribution {
  require(
    clustering != Nil,
    "The clustering expressions of a ClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")
}

object ClusteredDistribution {
  def apply(clustering: Seq[Expression]): ClusteredDistribution =
    ClusteredDistribution(clustering, nullSafe = true)
}

/**
 * Represents data where tuples have been ordered according to the `ordering`
 * [[Expression Expressions]].  This is a strictly stronger guarantee than
 * [[ClusteredDistribution]] as an ordering will ensure that tuples that share the
 * same value for the ordering expressions are contiguous and will never be split across
 * partitions.
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  require(
    ordering != Nil,
    "The ordering expressions of a OrderedDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  // TODO: This is not really valid...
  def clustering: Set[Expression] = ordering.map(_.child).toSet
}

sealed trait Partitioning {
  /** Returns the number of partitions that the data is split across */
  val numPartitions: Int

  /**
   * Returns true iff the guarantees made by this [[Partitioning]] are sufficient
   * to satisfy the partitioning scheme mandated by the `required` [[Distribution]],
   * i.e. the current dataset does not need to be re-partitioned for the `required`
   * Distribution (it is possible that tuples within a partition need to be reorganized).
   */
  def satisfies(required: Distribution): Boolean

  /**
   * Returns true iff all distribution guarantees made by this partitioning can also be made
   * for the `other` specified partitioning.
   * For example, two [[HashPartitioning HashPartitioning]]s are
   * only compatible if the `numPartitions` of them is the same.
   */
  def compatibleWith(other: Partitioning): Boolean

  def guarantees(other: Partitioning): Boolean

  /** Returns the expressions that are used to key the partitioning. */
  def keyExpressions: Seq[Expression]

  def withNullSafeSetting(newNullSafe: Boolean): Partitioning
}

case class UnknownPartitioning(numPartitions: Int) extends Partitioning {
  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case _ => false
  }

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case UnknownPartitioning(_) => true
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = false

  override def keyExpressions: Seq[Expression] = Nil

  override def withNullSafeSetting(newNullSafe: Boolean): Partitioning = this
}

case object SinglePartition extends Partitioning {
  val numPartitions = 1

  override def satisfies(required: Distribution): Boolean = true

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case SinglePartition => true
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = other match {
    case SinglePartition => true
    case _ => false
  }

  override def keyExpressions: Seq[Expression] = Nil

  override def withNullSafeSetting(newNullSafe: Boolean): Partitioning = this
}

case object BroadcastPartitioning extends Partitioning {
  val numPartitions = 1

  override def satisfies(required: Distribution): Boolean = true

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case SinglePartition => true
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = other match {
    case BroadcastPartitioning => true
    case _ => false
  }

  override def keyExpressions: Seq[Expression] = Nil

  override def withNullSafeSetting(newNullSafe: Boolean): Partitioning = this
}

/**
 * Represents a partitioning where rows are split up across partitions based on the hash
 * of `expressions`.  All rows where `expressions` evaluate to the same values are guaranteed to be
 * in the same partition. When `nullSafe` is true, for two null values in two rows evaluated
 * by `clustering`, we consider these two nulls are equal.
 */
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int, nullSafe: Boolean)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  lazy val clusteringSet = expressions.toSet

  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case ClusteredDistribution(requiredClustering, _) if nullSafe =>
      clusteringSet.subsetOf(requiredClustering.toSet)
    case ClusteredDistribution(requiredClustering, false) if !nullSafe =>
      clusteringSet.subsetOf(requiredClustering.toSet)
    case _ => false
  }

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case BroadcastPartitioning => true
    case h: HashPartitioning if h == this => true
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = other match {
    case o: HashPartitioning if nullSafe =>
      this.clusteringSet == o.clusteringSet && this.numPartitions == o.numPartitions
    case o: HashPartitioning if !nullSafe && !o.nullSafe =>
      this.clusteringSet == o.clusteringSet && this.numPartitions == o.numPartitions
    case _ => false
  }

  override def keyExpressions: Seq[Expression] = expressions

  override def withNullSafeSetting(newNullSafe: Boolean): Partitioning =
    HashPartitioning(expressions, numPartitions, newNullSafe)
}

object HashPartitioning {
  def apply(expressions: Seq[Expression], numPartitions: Int): HashPartitioning =
    HashPartitioning(expressions, numPartitions, nullSafe = true)
}

/**
 * Represents a partitioning where rows are split across partitions based on some total ordering of
 * the expressions specified in `ordering`.  When data is partitioned in this manner the following
 * two conditions are guaranteed to hold:
 *  - All row where the expressions in `ordering` evaluate to the same values will be in the same
 *    partition.
 *  - Each partition will have a `min` and `max` row, relative to the given ordering.  All rows
 *    that are in between `min` and `max` in this `ordering` will reside in this partition.
 *
 * This class extends expression primarily so that transformations over expression will descend
 * into its child.
 */
case class RangePartitioning(ordering: Seq[SortOrder], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[SortOrder] = ordering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  private[this] lazy val clusteringSet = ordering.map(_.child).toSet

  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case OrderedDistribution(requiredOrdering) =>
      val minSize = Seq(requiredOrdering.size, ordering.size).min
      requiredOrdering.take(minSize) == ordering.take(minSize)
    case ClusteredDistribution(requiredClustering, _) =>
      clusteringSet.subsetOf(requiredClustering.toSet)
    case _ => false
  }

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case BroadcastPartitioning => true
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = other match {
    case o: RangePartitioning => this == o
    case _ => false
  }

  override def keyExpressions: Seq[Expression] = ordering.map(_.child)

  override def withNullSafeSetting(newNullSafe: Boolean): Partitioning = this
}

/**
 * A collection of [[Partitioning]]s.
 */
case class PartitioningCollection(partitionings: Seq[Partitioning])
  extends Expression with Partitioning with Unevaluable {

  require(
    partitionings.map(_.numPartitions).distinct.length == 1,
    s"PartitioningCollection requires all of its partitionings have the same numPartitions.")

  override def children: Seq[Expression] = partitionings.collect {
    case expr: Expression => expr
  }

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override val numPartitions = partitionings.map(_.numPartitions).distinct.head

  override def satisfies(required: Distribution): Boolean =
    partitionings.exists(_.satisfies(required))

  override def compatibleWith(other: Partitioning): Boolean =
    partitionings.exists(_.compatibleWith(other))

  override def guarantees(other: Partitioning): Boolean =
    partitionings.exists(_.guarantees(other))

  override def keyExpressions: Seq[Expression] = partitionings.head.keyExpressions

  override def withNullSafeSetting(newNullSafe: Boolean): Partitioning = {
    PartitioningCollection(partitionings.map(_.withNullSafeSetting(newNullSafe)))
  }

  override def toString: String = {
    partitionings.map(_.toString).mkString("(", " or ", ")")
  }
}
