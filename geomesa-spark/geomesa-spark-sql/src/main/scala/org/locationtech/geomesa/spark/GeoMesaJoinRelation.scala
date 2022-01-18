/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ScalaUDF}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.sweepline.{SweepLineIndex, SweepLineInterval}
import org.opengis.feature.simple.SimpleFeature

// A special case relation that is built when a join happens across two identically partitioned relations
// Uses the sweepline algorithm to lower the complexity of the join
case class GeoMesaJoinRelation(
    sqlContext: SQLContext,
    leftRel: GeoMesaRelation,
    rightRel: GeoMesaRelation,
    schema: StructType,
    condition: Expression,
    filt: org.opengis.filter.Filter = org.opengis.filter.Filter.INCLUDE,
    props: Option[Seq[String]] = None
  ) extends BaseRelation with PrunedFilteredScan {

  import RelationUtils.CoordinateOrdering

  private val leftPartitioning = leftRel.partitioned.getOrElse {
    throw new IllegalArgumentException("Trying to join un-partitioned relations")
  }
  private val rightPartitioning = rightRel.partitioned.getOrElse {
    throw new IllegalArgumentException("Trying to join un-partitioned relations")
  }

  private def sweeplineJoin(overlapAction: OverlapAction): RDD[(Int, (SimpleFeature, SimpleFeature))] = {
    leftPartitioning.rdd.join(rightPartitioning.rdd).flatMap { case (key, (left, right)) =>
      val sweeplineIndex = new SweepLineIndex()
      left.foreach {feature =>
        val coords = feature.getDefaultGeometry.asInstanceOf[Geometry].getCoordinates
        sweeplineIndex.add(new SweepLineInterval(coords.min.x, coords.max.x, (0, feature)))
      }
      right.foreach {feature =>
        val coords = feature.getDefaultGeometry.asInstanceOf[Geometry].getCoordinates
        sweeplineIndex.add(new SweepLineInterval(coords.min.x, coords.max.x, (1, feature)))
      }
      sweeplineIndex.computeOverlaps(overlapAction)
      overlapAction.joinList.map(key -> _)
    }
  }

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {

    val leftSchema = leftRel.schema
    val rightSchema = rightRel.schema
    val leftExtractors = SparkUtils.getExtractors(leftSchema.fieldNames, leftSchema)
    val rightExtractors = SparkUtils.getExtractors(rightSchema.fieldNames, rightSchema)
    val joinedSchema = StructType(leftSchema.fields ++ rightSchema.fields)
    val joinedExtractors = leftExtractors ++ rightExtractors

    // Extract geometry indexes and spatial function from condition expression and relation SFTs
    val (leftIndex, rightIndex, conditionFunction) = {
      val scalaUdf = condition.asInstanceOf[ScalaUDF]
      val function = scalaUdf.function.asInstanceOf[(Geometry, Geometry) => Boolean]
      val children = scalaUdf.children.asInstanceOf[Seq[AttributeReference]]
      // Because the predicate may not have parameters in the right order, we must check both
      val leftAttr = children.head.name
      val rightAttr = children(1).name
      val leftIndex = leftRel.sft.indexOf(leftAttr)
      if (leftIndex == -1) {
        (leftRel.sft.indexOf(rightAttr), rightRel.sft.indexOf(leftAttr), function)
      } else {
        (leftIndex, rightRel.sft.indexOf(rightAttr), function)
      }
    }

    // Perform the sweepline join and build rows containing matching features
    val overlapAction = new OverlapAction(leftIndex, rightIndex, conditionFunction)
    sweeplineJoin(overlapAction).mapPartitions { iter =>
      iter.map { case (_, (leftFeature, rightFeature)) =>
        SparkUtils.joinedSf2row(joinedSchema, leftFeature, rightFeature, joinedExtractors)
      }
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case t @ (_:IsNotNull | _:IsNull) => true
      case _ => false
    }
  }
}
