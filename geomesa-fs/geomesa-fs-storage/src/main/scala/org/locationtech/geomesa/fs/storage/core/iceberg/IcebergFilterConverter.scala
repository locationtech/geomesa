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
import org.locationtech.geomesa.fs.storage.core.schema.{BoundingBoxField, ColumnName}
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
  def apply(sft: SimpleFeatureType, filter: Filter): ReadFilter = {
    if (filter == Filter.INCLUDE) {
      ReadFilter(Expressions.alwaysTrue(), None, Set.empty)
    } else if (filter == Filter.EXCLUDE) {
      ReadFilter(Expressions.alwaysFalse(), None, Set.empty)
    } else {
      val names = FilterHelper.propertyNames(filter).map(ColumnName.apply)
      names.foldLeft(ReadFilter(Expressions.alwaysTrue(), Some(filter), names.map(_.column).toSet))(reduce(sft))
    }
  }

  private def reduce(sft: SimpleFeatureType)(result: ReadFilter, name: ColumnName): ReadFilter = {
    val filter = result.remainder.orNull
    if (filter == null) {
      return result // no more filter to evaluate
    }
    val bindings = ObjectType.selectType(sft.getDescriptor(name.attribute))
    val predicate = bindings.head match {
      // note: non-points use repeated values, which aren't supported in parquet predicates
      case ObjectType.GEOMETRY => spatial(sft, name, filter)
      case ObjectType.DATE     => attribute[Date](sft, name, filter, Some(dateToMicros))
      case ObjectType.STRING   => attribute[String](sft, name, filter)
      case ObjectType.INT      => attribute[Integer](sft, name, filter)
      case ObjectType.LONG     => attribute[java.lang.Long](sft, name, filter)
      case ObjectType.FLOAT    => attribute[java.lang.Float](sft, name, filter)
      case ObjectType.DOUBLE   => attribute[java.lang.Double](sft, name, filter)
      case ObjectType.BOOLEAN  => attribute[java.lang.Boolean](sft, name, filter)
      case _ => ReadFilter(Expressions.alwaysTrue(), result.remainder, Set.empty)
    }
    ReadFilter(Expressions.and(predicate.expression, result.expression), predicate.remainder, predicate.columns ++ result.columns)
  }

  private def spatial(sft: SimpleFeatureType, name: ColumnName, filter: Filter): ReadFilter = {
    val (spatial, nonSpatial) = FilterExtractingVisitor(filter, name.attribute, sft, SpatialFilterStrategy.spatialCheck)
    val bounds = spatial.map(FilterHelper.extractGeometries(_, name.attribute))
    val xyBounds = bounds.toSeq.flatMap { extracted =>
      Seq(extracted).filter(e => e.nonEmpty && !e.disjoint).flatMap { e =>
        e.values.map(GeometryUtils.bounds)
      }
    }

    // filter against the bbox field
    val predicate = xyBounds.map { case (xmin, ymin, xmax, ymax) =>
      BoundingBoxField.filterIceberg(name.column, xmin, ymin, xmax, ymax)
    }

    val remaining = if (bounds.exists(_.precise)) { nonSpatial } else { Some(filter) }
    val geomCol = if (bounds.exists(b => !b.precise)) { Some(name.column) } else { None }
    val bboxCol = if (predicate.nonEmpty) { Some(BoundingBoxField.groupName(name.column)) } else { None }
    ReadFilter(predicate.reduce(Expressions.or), remaining, (geomCol ++ bboxCol).toSet)
  }

  private def attribute[T : ClassTag](
      sft: SimpleFeatureType,
      name: ColumnName,
      filter: Filter,
      transform: Option[T => Any] = None): ReadFilter = {
    val (attribute, nonAttribute) = FilterExtractingVisitor(filter, name.attribute, sft)
    val binding = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val bounds = attribute.map(FilterHelper.extractAttributeBounds(_, name.attribute, binding))
    val predicate = bounds.flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint && e.values.forall(_.isBounded)).map { e =>
        val values = transform match {
          case None => e.values
          case Some(t) =>
            e.values.map { bounds =>
              bounds.copy(bounds.lower.copy(bounds.lower.value.map(t.apply)), bounds.upper.copy(bounds.upper.value.map(t.apply)))
            }
        }
        val filters = values.map { bounds =>
          if (bounds.isEquals) {
            Expressions.equal(name.column, bounds.lower.value.get)
          } else {
            val lower = bounds.lower.value.map { value =>
              if (bounds.lower.inclusive) { Expressions.greaterThanOrEqual(name.column, value) } else { Expressions.greaterThan(name.column, value) }
            }
            val upper = bounds.upper.value.map { value =>
              if (bounds.upper.inclusive) { Expressions.lessThanOrEqual(name.column, value) } else { Expressions.lessThan(name.column, value) }
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
    val cols = if (predicate.isDefined || bounds.exists(b => !b.precise)) { Set(name.column) } else { Set.empty[String] }
    ReadFilter(predicate.getOrElse(Expressions.alwaysTrue()), remaining, cols)
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

  /**
   * Parts of a filter
   *
   * @param expression expression to apply to the scan
   * @param remainder remaining cql filter that isn't captured by the expression
   * @param columns column names (encoded) needed for evaluating both the expression and filter
   */
  case class ReadFilter(expression: Expression, remainder: Option[Filter], columns: Set[String])
}
