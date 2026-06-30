/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet

import org.apache.parquet.filter2.predicate.Operators._
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.fs.storage.core.schema.{BoundingBoxField, ColumnName}
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, ObjectType}

import java.util.Date
import scala.reflect.ClassTag

object ParquetFilterConverter {

  def convert(sft: SimpleFeatureType, filter: Filter): (Option[FilterPredicate], Option[Filter]) = {
    if (filter == Filter.INCLUDE) { (None, None) } else {
      FilterHelper.propertyNames(filter).foldLeft((Option.empty[FilterPredicate], Option(filter)))(reduce(sft))
    }
  }

  private def reduce(
      sft: SimpleFeatureType
    )(result: (Option[FilterPredicate], Option[Filter]),
      name: String): (Option[FilterPredicate], Option[Filter]) = {
    val (parquet, geotools) = result
    val filter = geotools.orNull
    if (filter == null) {
      return result // no more filter to evaluate
    }

    val bindings = ObjectType.selectType(sft.getDescriptor(name))
    val col = ColumnName(name)

    val (predicate, remaining): (Option[FilterPredicate], Option[Filter]) = bindings.head match {
      // note: non-points use repeated values, which aren't supported in parquet predicates
      case ObjectType.GEOMETRY => spatial(sft, col, filter)
      case ObjectType.DATE     => attribute(sft, col.attribute, filter, FilterApi.longColumn(col.column), toMicros)
      case ObjectType.STRING   => attribute(sft, col.attribute, filter, FilterApi.binaryColumn(col.column), Binary.fromString)
      case ObjectType.INT      => attribute(sft, col.attribute, filter, FilterApi.intColumn(col.column), identity[java.lang.Integer])
      case ObjectType.LONG     => attribute(sft, col.attribute, filter, FilterApi.longColumn(col.column), identity[java.lang.Long])
      case ObjectType.FLOAT    => attribute(sft, col.attribute, filter, FilterApi.floatColumn(col.column), identity[java.lang.Float])
      case ObjectType.DOUBLE   => attribute(sft, col.attribute, filter, FilterApi.doubleColumn(col.column), identity[java.lang.Double])
      case ObjectType.BOOLEAN  => boolean(sft, col.attribute, filter, FilterApi.booleanColumn(col.column))
      case _ => (None, Some(filter))
    }

    ((predicate.toSeq ++ parquet).reduceLeftOption(FilterApi.and), remaining)
  }

  private def spatial(sft: SimpleFeatureType, name: ColumnName, filter: Filter): (Option[FilterPredicate], Option[Filter]) = {
    val (spatial, _) = FilterExtractingVisitor(filter, name.attribute, sft, SpatialFilterStrategy.spatialCheck)
    val xyBounds = spatial.map(FilterHelper.extractGeometries(_, name.attribute)).flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint).map { e =>
        e.values.map(GeometryUtils.bounds).reduce { (a, b) =>
          (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
        }
      }
    }

    val predicate = xyBounds.map { case (xmin, ymin, xmax, ymax) =>
      BoundingBoxField.filterParquet(name.column, xmin, ymin, xmax, ymax)
    }
    // since we don't know what the actual file encoding is up front, we always have to evaluate the full predicate post-read
    (predicate, Some(filter))
  }

  private def attribute[T : ClassTag, U <: Comparable[U]](
      sft: SimpleFeatureType,
      name: String,
      filter: Filter,
      col: Column[U] with SupportsLtGt,
      conversion: T => U): (Option[FilterPredicate], Option[Filter]) = {
    val (attribute, nonAttribute) = FilterExtractingVisitor(filter, name, sft)
    val binding = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val bounds = attribute.map(FilterHelper.extractAttributeBounds(_, name, binding))
    val predicate = bounds.flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint && e.values.forall(_.isBounded)).map { e =>
        val filters = e.values.map { bounds =>
          if (bounds.isEquals) {
            FilterApi.eq(col, conversion(bounds.lower.value.get))
          } else {
            val lower: Option[FilterPredicate] = bounds.lower.value.map { value =>
              val converted = conversion(value)
              if (bounds.lower.inclusive) { FilterApi.gtEq(col, converted) } else { FilterApi.gt(col, converted) }
            }
            val upper: Option[FilterPredicate] = bounds.upper.value.map { value =>
              val converted = conversion(value)
              if (bounds.upper.inclusive) { FilterApi.ltEq(col, converted) } else { FilterApi.lt(col, converted) }
            }
            (lower, upper) match {
              case (Some(lo), Some(hi)) => FilterApi.and(lo, hi)
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
    (predicate, remaining)
  }

  private def boolean(
      sft: SimpleFeatureType,
      name: String,
      filter: Filter,
      col: BooleanColumn): (Option[FilterPredicate], Option[Filter]) = {
    val (attribute, nonAttribute) = FilterExtractingVisitor(filter, name, sft)
    val bounds = attribute.map(FilterHelper.extractAttributeBounds(_, name, classOf[java.lang.Boolean]))
    val predicate = bounds.flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint && e.values.forall(_.isEquals)).map { e =>
        e.values.map(bounds => FilterApi.eq(col, bounds.lower.value.get)).reduce(FilterApi.or)
      }
    }
    val remaining = if (bounds.exists(_.precise)) { nonAttribute } else { Some(filter) }
    (predicate, remaining)
  }

  private def toMicros(date: Date): java.lang.Long = Long.box(date.getTime * 1000L)

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
  private def merge[T <: Comparable[T]](filters: Seq[FilterPredicate]): FilterPredicate = {
    lazy val values = filters.collect {
      case f: Lt[T] => (f.getColumn.asInstanceOf[Column[T] with SupportsEqNotEq], f.getValue)
      case f: Gt[T] => (f.getColumn.asInstanceOf[Column[T] with SupportsEqNotEq], f.getValue)
    }.distinct

    if (filters.lengthCompare(2) == 0 &&
        filters.exists(_.isInstanceOf[Lt[_]]) &&
        filters.exists(_.isInstanceOf[Gt[_]]) &&
        values.lengthCompare(1) == 0) {
      FilterApi.notEq(values.head._1, values.head._2)
    } else {
      filters.reduce(FilterApi.or)
    }
  }
}
