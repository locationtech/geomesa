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

import scala.reflect.ClassTag

object IcebergFilterConverter {

  def apply(sft: SimpleFeatureType, filter: Filter): Expression = {
    if (filter == Filter.INCLUDE) {
      Expressions.alwaysTrue()
    } else if (filter == Filter.EXCLUDE) {
      Expressions.alwaysFalse()
    } else {
      val names = FilterHelper.propertyNames(filter)
      names.foldLeft[Expression](Expressions.alwaysTrue()) { case (e, name) => Expressions.and(e, convert(sft, filter, name)) }
    }
  }

  private def convert(sft: SimpleFeatureType, filter: Filter, name: String): Expression = {
    val bindings = ObjectType.selectType(sft.getDescriptor(name))
    bindings.head match {
      // note: non-points use repeated values, which aren't supported in parquet predicates
      case ObjectType.GEOMETRY => spatial(sft, name, filter)
      case ObjectType.DATE     => attribute(sft, name, filter)
      case ObjectType.STRING   => attribute(sft, name, filter)
      case ObjectType.INT      => attribute(sft, name, filter)
      case ObjectType.LONG     => attribute(sft, name, filter)
      case ObjectType.FLOAT    => attribute(sft, name, filter)
      case ObjectType.DOUBLE   => attribute(sft, name, filter)
      case ObjectType.BOOLEAN  => attribute(sft, name, filter)
      case _ => Expressions.alwaysTrue()
    }
  }

  private def spatial(sft: SimpleFeatureType, name: String, filter: Filter): Expression = {
    val (spatial, _) = FilterExtractingVisitor(filter, name, sft, SpatialFilterStrategy.spatialCheck)
    val xyBounds = spatial.map(FilterHelper.extractGeometries(_, name)).flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint).map { e =>
        e.values.map(GeometryUtils.bounds).reduce { (a, b) =>
          (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
        }
      }
    }

    // filter against the bbox field
    val bboxGroup = BoundingBoxField(name).bbox
    val predicate = xyBounds.map { case (xmin, ymin, xmax, ymax) =>
      Seq(
        Expressions.lessThanOrEqual(s"$bboxGroup.${BoundingBoxField.XMin}", Float.box(xmax.toFloat)),
        Expressions.greaterThanOrEqual(s"$bboxGroup.${BoundingBoxField.XMax}", Float.box(xmin.toFloat)),
        Expressions.lessThanOrEqual(s"$bboxGroup.${BoundingBoxField.YMin}", Float.box(ymax.toFloat)),
        Expressions.greaterThanOrEqual(s"$bboxGroup.${BoundingBoxField.YMax}", Float.box(ymin.toFloat))
      ).reduce(Expressions.and)
    }
    predicate.reduce(Expressions.or)
  }

  private def attribute[T : ClassTag](sft: SimpleFeatureType, name: String, filter: Filter): Expression = {
    val (attribute, _) = FilterExtractingVisitor(filter, name, sft)
    val binding = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val bounds = attribute.map(FilterHelper.extractAttributeBounds(_, name, binding))
    val predicate = bounds.flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint && e.values.forall(_.isBounded)).map { e =>
        val col = ColumnName(name)
        val filters = e.values.map { bounds =>
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
    predicate.getOrElse(Expressions.alwaysTrue())
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
}
