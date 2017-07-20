/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import org.apache.spark.Partition
import org.apache.spark.rdd.{CartesianPartition, CartesianRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, UnsafeRow}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/* A more efficient CartesianRDD that only constructs
 * CartesianPartitions for matching indexes in the two RDDs
 * i.e. 0 -> 0, 1 -> 1, 2 -> 2 as opposed to a cross-product.
 */
class TargetedCartesianRDD(left : RDD[UnsafeRow], right : RDD[UnsafeRow], numFieldsOfRight: Int)
  extends CartesianRDD[UnsafeRow, UnsafeRow](left.sparkContext, left, right) {

  val numPartitions: Int = rdd1.partitions.length

  // Instead of doing a cross product here, we know a one-to-one mapping will suffice
  override def getPartitions: Array[Partition] = {
    val array = Array.ofDim[Partition](numPartitions)
    for (index <- 0 until numPartitions) {
      array(index) = new CartesianPartition(index, rdd1, rdd2, index, index)
    }
    array
  }
}

/* A special catalyst tree node that uses a
 * TargetedCartesianRDD to compute the join.
 * This can be done when the two rdds are spatially
 * partitioned by the same partitioning scheme
 */
case class PartitionedJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends BinaryExecNode {

  override def output: Seq[Attribute] = left.output ++ right.output

  override private[sql] lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val leftResults = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResults = right.execute().asInstanceOf[RDD[UnsafeRow]]

    val pair = new TargetedCartesianRDD(leftResults, rightResults, right.output.size)
    pair.mapPartitionsInternal { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      val filtered = if (condition.isDefined) {
        val boundCondition: (InternalRow) => Boolean =
          newPredicate(condition.get, left.output ++ right.output)
        val joined = new JoinedRow

        iter.filter { r =>
          boundCondition(joined(r._1, r._2))
        }
      } else {
        iter
      }
      filtered.map { r =>
        numOutputRows += 1
        joiner.join(r._1, r._2)
      }
    }
  }
}
