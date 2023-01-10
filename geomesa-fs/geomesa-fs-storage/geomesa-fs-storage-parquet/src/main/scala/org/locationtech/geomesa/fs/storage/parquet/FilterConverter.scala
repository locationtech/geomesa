/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet

import org.apache.parquet.filter2.predicate.Operators._
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, ObjectType}
import org.locationtech.geomesa.utils.text.StringSerialization

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
      case ObjectType.GEOMETRY if bindings.last == ObjectType.POINT => spatial(sft, name, filter, col)
      case ObjectType.DATE    => temporal(sft, name, filter, FilterApi.longColumn(col))
      case ObjectType.STRING  => attribute(sft, name, filter, FilterApi.binaryColumn(col), Binary.fromString)
      case ObjectType.INT     => attribute(sft, name, filter, FilterApi.intColumn(col), identity[java.lang.Integer])
      case ObjectType.LONG    => attribute(sft, name, filter, FilterApi.longColumn(col), identity[java.lang.Long])
      case ObjectType.FLOAT   => attribute(sft, name, filter, FilterApi.floatColumn(col), identity[java.lang.Float])
      case ObjectType.DOUBLE  => attribute(sft, name, filter, FilterApi.doubleColumn(col), identity[java.lang.Double])
      case ObjectType.BOOLEAN => boolean(sft, name, filter, FilterApi.booleanColumn(col))
      case _ => (None, Some(filter))
    }

    ((predicate.toSeq ++ parquet).reduceLeftOption(FilterApi.and), remaining)
  }

  private def spatial(
      sft: SimpleFeatureType,
      name: String,
      filter: Filter,
      col: String): (Option[FilterPredicate], Option[Filter]) = {
    val (spatial, nonSpatial) = FilterExtractingVisitor(filter, name, sft, SpatialFilterStrategy.spatialCheck)
    val predicate = spatial.map(FilterHelper.extractGeometries(_, name)).flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint).map { e =>
        val xy = e.values.map(GeometryUtils.bounds).reduce { (a, b) =>
          (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
        }
        val xcol = FilterApi.doubleColumn(s"$col.x")
        val ycol = FilterApi.doubleColumn(s"$col.y")
        val filters = Seq[FilterPredicate](
          FilterApi.gtEq(xcol, Double.box(xy._1)),
          FilterApi.gtEq(ycol, Double.box(xy._2)),
          FilterApi.ltEq(xcol, Double.box(xy._3)),
          FilterApi.ltEq(ycol, Double.box(xy._4))
        )
        filters.reduce(FilterApi.and)
      }
    }
    (predicate, nonSpatial)
  }

  private def temporal(
      sft: SimpleFeatureType,
      name: String,
      filter: Filter,
      col: LongColumn): (Option[FilterPredicate], Option[Filter]) = {
    val (temporal, nonTemporal) = FilterExtractingVisitor(filter, name, sft)
    val predicate = temporal.map(FilterHelper.extractIntervals(_, name)).flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint && e.forall(_.isBounded)).map { e =>
        val filters = e.values.map { bounds =>
          if (bounds.isEquals) {
            FilterApi.eq(col, Long.box(bounds.lower.value.get.toInstant.toEpochMilli))
          } else {
            val lower: Option[FilterPredicate] = bounds.lower.value.map { value =>
              val long = Long.box(value.toInstant.toEpochMilli)
              if (bounds.lower.inclusive) { FilterApi.gtEq(col, long) } else { FilterApi.gt(col, long) }
            }
            val upper: Option[FilterPredicate] = bounds.upper.value.map { value =>
              val long = Long.box(value.toInstant.toEpochMilli)
              if (bounds.upper.inclusive) { FilterApi.ltEq(col, long) } else { FilterApi.lt(col, long) }
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
    (predicate, nonTemporal)
  }

  private def attribute[T : ClassTag, U <: Comparable[U]](
      sft: SimpleFeatureType,
      name: String,
      filter: Filter,
      col: Column[U] with SupportsLtGt,
      conversion: T => U): (Option[FilterPredicate], Option[Filter]) = {
    val (attribute, nonAttribute) = FilterExtractingVisitor(filter, name, sft)
    val binding = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val predicate = attribute.map(FilterHelper.extractAttributeBounds(_, name, binding)).flatMap { extracted =>
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
    (predicate, nonAttribute)
  }

  private def boolean(
      sft: SimpleFeatureType,
      name: String,
      filter: Filter,
      col: BooleanColumn): (Option[FilterPredicate], Option[Filter]) = {
    val (attribute, nonAttribute) = FilterExtractingVisitor(filter, name, sft)
    val predicate = attribute.map(FilterHelper.extractAttributeBounds(_, name, classOf[java.lang.Boolean])).flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint && e.values.forall(_.isEquals)).map { e =>
        e.values.map(bounds => FilterApi.eq(col, bounds.lower.value.get)).reduce(FilterApi.or)
      }
    }
    (predicate, nonAttribute)
  }

  /**
    * Merge OR'd filters
    *
    * Detect and re-write "not equals" filters to handle null values
    *
    * "foo != x" comes out as "foo < x OR foo > x"
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
