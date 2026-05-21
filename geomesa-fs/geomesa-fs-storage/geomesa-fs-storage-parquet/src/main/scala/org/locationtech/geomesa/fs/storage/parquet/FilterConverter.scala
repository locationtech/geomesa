/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet

import org.apache.parquet.filter2.predicate.Operators._
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.{XZ2Encoder, Z2Encoder}
import org.locationtech.geomesa.fs.storage.parquet.io.geometry.ZValues.ZValueField
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, ObjectType}
import org.locationtech.geomesa.utils.text.StringSerialization

import java.util.Date
import scala.reflect.ClassTag

object FilterConverter {

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
    val col = StringSerialization.alphaNumericSafeString(name)

    val (predicate, remaining): (Option[FilterPredicate], Option[Filter]) = bindings.head match {
      // note: non-points use repeated values, which aren't supported in parquet predicates
      case ObjectType.GEOMETRY => spatial(sft, name, filter, col, bindings.last)
      case ObjectType.DATE     => attribute(sft, name, filter, FilterApi.longColumn(col), toMicros)
      case ObjectType.STRING   => attribute(sft, name, filter, FilterApi.binaryColumn(col), Binary.fromString)
      case ObjectType.INT      => attribute(sft, name, filter, FilterApi.intColumn(col), identity[java.lang.Integer])
      case ObjectType.LONG     => attribute(sft, name, filter, FilterApi.longColumn(col), identity[java.lang.Long])
      case ObjectType.FLOAT    => attribute(sft, name, filter, FilterApi.floatColumn(col), identity[java.lang.Float])
      case ObjectType.DOUBLE   => attribute(sft, name, filter, FilterApi.doubleColumn(col), identity[java.lang.Double])
      case ObjectType.BOOLEAN  => boolean(sft, name, filter, FilterApi.booleanColumn(col))
      case _ => (None, Some(filter))
    }

    ((predicate.toSeq ++ parquet).reduceLeftOption(FilterApi.and), remaining)
  }

  private def spatial(
      sft: SimpleFeatureType,
      name: String,
      filter: Filter,
      col: String,
      typed: ObjectType): (Option[FilterPredicate], Option[Filter]) = {
    val (spatial, _) = FilterExtractingVisitor(filter, name, sft, SpatialFilterStrategy.spatialCheck)
    val predicate = spatial.map(FilterHelper.extractGeometries(_, name)).flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint).map { e =>
        // compute our ranges based on the coarse bounds for our query
        val multiplier = QueryProperties.PolygonDecompMultiplier.toInt.get
        val bits = QueryProperties.PolygonDecompBits.toInt.get
        val bounds = e.values.flatMap(GeometryUtils.bounds(_, multiplier, bits))
        val (field, ranges) =
          if (typed == ObjectType.POINT) {
            val field = ZValueField.z2(col, encoded = true).zValue
            val ranges = Z2Encoder.sfc.ranges(bounds, 64, Some(8)) // TODO make configurable
            (field, ranges)
          } else {
            val field = ZValueField.xz2(col, encoded = true).zValue
            val ranges = XZ2Encoder.sfc.ranges(bounds, Some(8))
            (field, ranges)
          }

        val zcol = FilterApi.longColumn(field)
        val filters = ranges.map(r => FilterApi.and(FilterApi.gtEq(zcol, Long.box(r.lower)), FilterApi.ltEq(zcol, Long.box(r.upper))))
        filters.reduce(FilterApi.or)
      }
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
