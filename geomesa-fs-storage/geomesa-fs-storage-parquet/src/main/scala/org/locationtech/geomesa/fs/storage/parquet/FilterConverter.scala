/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet

import org.apache.parquet.filter2.predicate.FilterPredicate.Visitor
import org.apache.parquet.filter2.predicate.Operators._
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, UserDefinedPredicate}
import org.apache.parquet.io.api.Binary
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.BoundingBoxField
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
      case ObjectType.DATE     => temporal(sft, name, filter, FilterApi.longColumn(col))
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
    val xyBounds = spatial.map(FilterHelper.extractGeometries(_, name)).flatMap { extracted =>
      Some(extracted).filter(e => e.nonEmpty && !e.disjoint).map { e =>
        e.values.map(GeometryUtils.bounds).reduce { (a, b) =>
          (math.min(a._1, b._1), math.min(a._2, b._2), math.max(a._3, b._3), math.max(a._4, b._4))
        }
      }
    }

    val predicate = xyBounds.map { case (xmin, ymin, xmax, ymax) =>
      // filter against the bbox field
      val bboxGroup = BoundingBoxField(col, encoded = true).bbox
      val bboxXminCol = FilterApi.floatColumn(s"$bboxGroup.${BoundingBoxField.XMin}")
      val bbox = {
        val yminCol = FilterApi.floatColumn(s"$bboxGroup.${BoundingBoxField.YMin}")
        val xmaxCol = FilterApi.floatColumn(s"$bboxGroup.${BoundingBoxField.XMax}")
        val ymaxCol = FilterApi.floatColumn(s"$bboxGroup.${BoundingBoxField.YMax}")
        Seq[FilterPredicate](
          FilterApi.ltEq(bboxXminCol, Float.box(xmax.toFloat)),
          FilterApi.gtEq(xmaxCol, Float.box(xmin.toFloat)),
          FilterApi.ltEq(yminCol, Float.box(ymax.toFloat)),
          FilterApi.gtEq(ymaxCol, Float.box(ymin.toFloat))
        ).reduce(FilterApi.and)
      }
      if (typed == ObjectType.POINT) {
        // point types that are natively encoded don't have bbox fields, as we can filter on them directly
        val xcol = FilterApi.doubleColumn(s"$col.x")
        val ycol = FilterApi.doubleColumn(s"$col.y")
        val xy = Seq[FilterPredicate](
          FilterApi.gtEq(xcol, Double.box(xmin)),
          FilterApi.gtEq(ycol, Double.box(ymin)),
          FilterApi.ltEq(xcol, Double.box(xmax)),
          FilterApi.ltEq(ycol, Double.box(ymax))
        ).reduce(FilterApi.and)
        // if x/y don't exist, then the bbox will (in WKB encoding)
        FilterApi.or(bbox, xy)
      } else {
        // add null for back-compatibility with files that don't contain bbox cols
        FilterApi.or(FilterApi.eq(bboxXminCol, null.asInstanceOf[java.lang.Float]), bbox)
      }
    }
    // since we don't know what the actual file encoding is up front, we always have to evaluate the full predicate post-read
    (predicate, Some(filter))
  }

  private def temporal(
      sft: SimpleFeatureType,
      name: String,
      filter: Filter,
      col: LongColumn): (Option[FilterPredicate], Option[Filter]) = {
    val (predicate, remaining) = attribute[Date, java.lang.Long](sft, name, filter, col, _.getTime)
    // note: we need to account for both millisecond and microsecond encoded dates
    // since the reasonable overlap of the two is pretty non-existent, we just use an OR of both potential ranges
    val millisOrMicros = predicate.map(p => FilterApi.or(p, p.accept(MillisToMicrosVisitor)))
    (millisOrMicros, remaining)
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

  /**
   * Visitor that replaces all millisecond-encoded date values with microsecond-encoded ones. Note that it is not
   * robust, in that it only works if all values are Longs
   */
  private object MillisToMicrosVisitor extends Visitor[FilterPredicate] {

    import scala.collection.JavaConverters._

    private def millisToMicros[T <: Comparable[T]](value: T): T =
      Long.box(value.asInstanceOf[java.lang.Long] * 1000L).asInstanceOf[T]

    override def visit[T <: Comparable[T]](eq: Eq[T]): FilterPredicate =
      FilterApi.eq(eq.getColumn.asInstanceOf[Column[T] with SupportsEqNotEq], millisToMicros(eq.getValue))
    override def visit[T <: Comparable[T]](notEq: NotEq[T]): FilterPredicate =
      FilterApi.notEq(notEq.getColumn.asInstanceOf[Column[T] with SupportsEqNotEq], millisToMicros(notEq.getValue))
    override def visit[T <: Comparable[T]](lt: Lt[T]): FilterPredicate =
      FilterApi.lt(lt.getColumn.asInstanceOf[Column[T] with SupportsLtGt], millisToMicros(lt.getValue))
    override def visit[T <: Comparable[T]](ltEq: LtEq[T]): FilterPredicate =
      FilterApi.ltEq(ltEq.getColumn.asInstanceOf[Column[T] with SupportsLtGt], millisToMicros(ltEq.getValue))
    override def visit[T <: Comparable[T]](gt: Gt[T]): FilterPredicate =
      FilterApi.gt(gt.getColumn.asInstanceOf[Column[T] with SupportsLtGt], millisToMicros(gt.getValue))
    override def visit[T <: Comparable[T]](gtEq: GtEq[T]): FilterPredicate =
      FilterApi.gtEq(gtEq.getColumn.asInstanceOf[Column[T] with SupportsLtGt], millisToMicros(gtEq.getValue))
    override def visit(and: And): FilterPredicate = FilterApi.and(and.getLeft.accept(this), and.getRight.accept(this))
    override def visit(or: Or): FilterPredicate = FilterApi.or(or.getLeft.accept(this), or.getRight.accept(this))
    override def visit(not: Not): FilterPredicate = FilterApi.not(not.getPredicate.accept(this))
    override def visit[T <: Comparable[T]](in: In[T]): FilterPredicate =
      FilterApi.in(in.getColumn.asInstanceOf[Column[T] with SupportsEqNotEq], in.getValues.asScala.map(millisToMicros).asJava)
    override def visit[T <: Comparable[T]](notIn: NotIn[T]): FilterPredicate =
      FilterApi.notIn(notIn.getColumn.asInstanceOf[Column[T] with SupportsEqNotEq], notIn.getValues.asScala.map(millisToMicros).asJava)
    override def visit[T <: Comparable[T]](contains: Contains[T]): FilterPredicate =
      throw new UnsupportedOperationException("visit Contains is not supported")
    override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: UserDefined[T, U]): FilterPredicate = udp
    override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](udp: LogicalNotUserDefined[T, U]): FilterPredicate = udp
  }
}
