/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.iceberg.expressions.Expression.Operation
import org.apache.iceberg.expressions.{Expression, Expressions, UnboundPredicate}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.fs.storage.core.parquet.schema.BoundingBoxes.BoundingBoxField
import org.locationtech.geomesa.fs.storage.core.parquet.schema.ColumnName
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, ObjectType}

import java.util.Date
import scala.reflect.ClassTag

object IcebergFilterConverter {

  /**
   * Returns an iceberg expression and a residual GeoTools filter that isn't captured by the expression (if any)
   *
   * @param sft simple feature type
   * @param filter geotools filter
   * @return
   */
  def apply(sft: SimpleFeatureType, filter: Filter): (Expression, Option[Filter]) = {
    if (filter == Filter.INCLUDE) {
      (Expressions.alwaysTrue(), None)
    } else if (filter == Filter.EXCLUDE) {
      (Expressions.alwaysFalse(), None)
    } else {
      val names = FilterHelper.propertyNames(filter)
      names.foldLeft[(Expression, Option[Filter])](Expressions.alwaysTrue(), Some(filter))(reduce(sft))
    }
  }

  private def reduce(sft: SimpleFeatureType)(result: (Expression, Option[Filter]), name: String): (Expression, Option[Filter]) = {
    val (iceberg, geotools) = result
    val filter = geotools.orNull
    if (filter == null) {
      return result // no more filter to evaluate
    }
    val bindings = ObjectType.selectType(sft.getDescriptor(name))
    val (predicate, remaining): (Expression, Option[Filter]) = bindings.head match {
      // note: non-points use repeated values, which aren't supported in parquet predicates
      case ObjectType.GEOMETRY => spatial(sft, name, filter)
      case ObjectType.DATE     => attribute[Date](sft, name, filter, Some(dateToMicros))
      case ObjectType.STRING   => attribute[String](sft, name, filter)
      case ObjectType.INT      => attribute[Integer](sft, name, filter)
      case ObjectType.LONG     => attribute[java.lang.Long](sft, name, filter)
      case ObjectType.FLOAT    => attribute[java.lang.Float](sft, name, filter)
      case ObjectType.DOUBLE   => attribute[java.lang.Double](sft, name, filter)
      case ObjectType.BOOLEAN  => attribute[java.lang.Boolean](sft, name, filter)
      case _ => (Expressions.alwaysTrue(), geotools)
    }
    (Expressions.and(predicate, iceberg), remaining)
  }

  private def spatial(sft: SimpleFeatureType, name: String, filter: Filter): (Expression, Option[Filter]) = {
    val (spatial, nonSpatial) = FilterExtractingVisitor(filter, name, sft, SpatialFilterStrategy.spatialCheck)
    val bounds = spatial.map(FilterHelper.extractGeometries(_, name))
    val xyBounds = bounds.flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint).map { e =>
        e.values.map(GeometryUtils.bounds).reduce { (a, b) =>
          (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
        }
      }
    }

    // filter against the bbox field
    val bboxGroup = BoundingBoxField(name).bbox
    val predicate = xyBounds.map { case (xmin, ymin, xmax, ymax) =>
      val exps = Seq(
        Expressions.lessThanOrEqual(s"$bboxGroup.${BoundingBoxField.XMin}", Float.box(xmax.toFloat)),
        Expressions.greaterThanOrEqual(s"$bboxGroup.${BoundingBoxField.XMax}", Float.box(xmin.toFloat)),
        Expressions.lessThanOrEqual(s"$bboxGroup.${BoundingBoxField.YMin}", Float.box(ymax.toFloat)),
        Expressions.greaterThanOrEqual(s"$bboxGroup.${BoundingBoxField.YMax}", Float.box(ymin.toFloat))
      )
      exps.reduce(Expressions.and)
    }

    val remaining = if (bounds.exists(_.precise)) { nonSpatial } else { Some(filter) }
    (predicate.reduce(Expressions.or), remaining)
  }

  private def attribute[T : ClassTag](
      sft: SimpleFeatureType,
      name: String,
      filter: Filter,
      transform: Option[T => Any] = None): (Expression, Option[Filter]) = {
    val (attribute, nonAttribute) = FilterExtractingVisitor(filter, name, sft)
    val binding = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val bounds = attribute.map(FilterHelper.extractAttributeBounds(_, name, binding))
    val predicate = bounds.flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint && e.values.forall(_.isBounded)).map { e =>
        val col = ColumnName(name)
        val values = transform match {
          case None => e.values
          case Some(t) =>
            e.values.map { bounds =>
              bounds.copy(bounds.lower.copy(bounds.lower.value.map(t.apply)), bounds.upper.copy(bounds.upper.value.map(t.apply)))
            }
        }
        val filters = values.map { bounds =>
          if (bounds.isEquals) {
            Expressions.equal(col, bounds.lower.value.get)
          } else {
            val lower = bounds.lower.value.map { value =>
              if (bounds.lower.inclusive) { Expressions.greaterThanOrEqual(col, value) } else { Expressions.greaterThan(col, value) }
            }
            val upper = bounds.upper.value.map { value =>
              if (bounds.upper.inclusive) { Expressions.lessThanOrEqual(col, value) } else { Expressions.lessThan(col, value) }
            }
            (lower, upper) match {
              case (Some(lo), Some(hi)) => Expressions.and(lo, hi)
              case (Some(f), None) => f
              case (None, Some(f)) => f
              case (None, None) => throw new IllegalStateException() // shouldn't happen due to checks above
            }
          }
        }
        merge(filters)
      }
    }
    val remaining = if (bounds.exists(_.precise)) { nonAttribute } else { Some(filter) }
    (predicate.getOrElse(Expressions.alwaysTrue()), remaining)
  }

  /**
   * Merge OR'd filters
   *
   * Detect and re-write "not equals" filters to handle null values
   *
   * FilterHelper methods make "foo != x" comes out as "foo < x OR foo > x"
   *
   * that won't return null values - not normally a concern since we don't index them in key-value dbs,
   * but we want to handle that case here
   *
   * @param filters filters
   * @return combined filter
   */
  private def merge(filters: Seq[Expression]): Expression = {
    lazy val values = filters.collect { case f: UnboundPredicate[_] => (f.ref().name(), f.literal().value()) }.distinct
    if (filters.lengthCompare(2) == 0 &&
      filters.exists(_.op() == Operation.LT) &&
      filters.exists(_.op == Operation.GT) &&
      values.lengthCompare(1) == 0) {
      Expressions.notEqual(values.head._1, values.head._2)
    } else {
      filters.reduce(Expressions.or)
    }
  }

  private def dateToMicros(date: Date): Long = date.getTime * 1000
}
