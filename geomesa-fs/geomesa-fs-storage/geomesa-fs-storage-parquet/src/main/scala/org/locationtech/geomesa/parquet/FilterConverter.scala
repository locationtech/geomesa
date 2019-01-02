/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import java.lang.{Boolean, Float, Long}
import java.time.{ZoneOffset, ZonedDateTime}

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.FilterHelper.extractGeometries
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.PropertyName

import scala.collection.JavaConversions._


class FilterConverter(sft: SimpleFeatureType) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
  protected val geomAttr: String = sft.getGeomField
  protected val dtgAttrOpt: Option[String] = sft.getDtgField
  private val ff = CommonFactoryFinder.getFilterFactory2

  private val MinDateTime = ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  private val MaxDateTime = ZonedDateTime.of(9999, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC)

  /**
    * Convert a geotools filter into a parquet filter and new partial geotools filter
    * to apply to parquet files for filtering
    *
    * @param f filter
    * @return a tuple representing the parquet filter and a residual geotools filter
    *         to apply for fine grained filtering since some of the predicates may
    *         be fully covered by the parquet filter
    */
  def convert(f: org.opengis.filter.Filter): (Option[FilterPredicate], org.opengis.filter.Filter) = {
    val filters = Seq(geoFilter(f), dtgFilter(f), attrFilter(f)).flatten
    if (filters.nonEmpty) {
      (Some(filters.reduceLeft(FilterApi.and)), residualFilter(f))
    } else {
      (None, f)
    }
  }

  // TODO do this in the single walk
  // TODO optimize by removing superfluous Filter.INCLUDE trees (collapse and/ors of Filter.INCLUDE)
  def residualFilter(f: org.opengis.filter.Filter): org.opengis.filter.Filter = {
    f match {
      case and: org.opengis.filter.And =>
        ff.and(and.getChildren.map(residualFilter))

      case or: org.opengis.filter.Or =>
        ff.or(or.getChildren.map(residualFilter))

      case binop: org.opengis.filter.BinaryComparisonOperator =>
        // These are all handled by the parquet attribute or date filter
        binop match {
          case _ if dtgAttrOpt.contains(binop.getExpression1.asInstanceOf[PropertyName].getPropertyName) =>
            org.opengis.filter.Filter.INCLUDE

          case _ @(_: org.opengis.filter.PropertyIsEqualTo |
                   _: org.opengis.filter.PropertyIsNotEqualTo |
                   _: org.opengis.filter.PropertyIsLessThan |
                   _: org.opengis.filter.PropertyIsLessThanOrEqualTo |
                   _: org.opengis.filter.PropertyIsGreaterThan |
                   _: org.opengis.filter.PropertyIsGreaterThanOrEqualTo) =>
            org.opengis.filter.Filter.INCLUDE
          case _ => f
        }
      case _ => f
    }
  }

  protected def dtgFilter(f: org.opengis.filter.Filter): Option[FilterPredicate] = {
    dtgAttrOpt.map { dtgAttr =>
      val filters = FilterHelper.extractIntervals(f, dtgAttr).values.map { bounds =>
        FilterApi.and(
          FilterApi.gtEq(FilterApi.longColumn(dtgAttr), Long.valueOf(bounds.lower.value.getOrElse(MinDateTime).toInstant.toEpochMilli)),
          FilterApi.ltEq(FilterApi.longColumn(dtgAttr), Long.valueOf(bounds.upper.value.getOrElse(MaxDateTime).toInstant.toEpochMilli))
        )
      }

      if (filters.nonEmpty) {
        Some(filters.reduceLeft(FilterApi.and))
      } else {
        None
      }
    }
  }.getOrElse(None)

  protected def geoFilter(f: org.opengis.filter.Filter): Option[FilterPredicate] = {
    val extracted = extractGeometries(f, geomAttr)
    if (extracted.isEmpty || extracted.disjoint) {
      None
    } else {
      val xy = extracted.values.map(GeometryUtils.bounds).reduce { (a, b) =>
        (math.min(a._1, b._1),
          math.min(a._2, b._2),
          math.max(a._3, b._3),
          math.max(a._4, b._4))
      }
      Some(
        List[FilterPredicate](
          FilterApi.gtEq(FilterApi.doubleColumn(s"$geomAttr.x"), Double.box(xy._1)),
          FilterApi.gtEq(FilterApi.doubleColumn(s"$geomAttr.y"), Double.box(xy._2)),
          FilterApi.ltEq(FilterApi.doubleColumn(s"$geomAttr.x"), Double.box(xy._3)),
          FilterApi.ltEq(FilterApi.doubleColumn(s"$geomAttr.y"), Double.box(xy._4))
       ).reduce(FilterApi.and)
      )
    }
  }

  protected def attrFilter(gtFilter: org.opengis.filter.Filter): Option[FilterPredicate] = {
    gtFilter match {

      case and: org.opengis.filter.And =>
        val res = and.getChildren.flatMap(attrFilter)
        if (res.nonEmpty) Option(res.reduceLeft(FilterApi.and)) else None

      case or: org.opengis.filter.Or =>
        val res = or.getChildren.flatMap(attrFilter)
        if (res.nonEmpty) Option(res.reduceLeft(FilterApi.or)) else None

      case binop: org.opengis.filter.BinaryComparisonOperator =>
        val name = binop.getExpression1.asInstanceOf[PropertyName].getPropertyName
        val value = binop.getExpression2.toString

        binop match {
          case _ if name == geomAttr | dtgAttrOpt.contains(name) =>
            None

          case _ =>
            val ad = sft.getDescriptor(name)
            val binding = ad.getType.getBinding
            val objectType: ObjectType = ObjectType.selectType(binding, ad.getUserData).head
            filter(objectType, binop, name, value)
        }

      case _ =>
        None
    }
  }

  def filter(objectType: ObjectType,
             binop: org.opengis.filter.BinaryComparisonOperator,
             name: String, value: AnyRef): Option[FilterPredicate] =
    objectType match {

      case ObjectType.STRING =>
        val col = FilterApi.binaryColumn(name)
        val conv = Binary.fromString(value.toString)
        binop match {
          case eq: org.opengis.filter.PropertyIsEqualTo =>
            Option(FilterApi.eq(col, conv))
          case neq: org.opengis.filter.PropertyIsNotEqualTo =>
            Option(FilterApi.notEq(col, conv))
          case lt: org.opengis.filter.PropertyIsLessThan =>
            Option(FilterApi.lt(col, conv))
          case lte: org.opengis.filter.PropertyIsLessThanOrEqualTo =>
            Option(FilterApi.ltEq(col, conv))
          case gt: org.opengis.filter.PropertyIsGreaterThan =>
            Option(FilterApi.gt(col, conv))
          case gte: org.opengis.filter.PropertyIsGreaterThanOrEqualTo =>
            Option(FilterApi.gtEq(col, conv))
          case _ =>
            None
        }

      case ObjectType.INT =>
        val col = FilterApi.intColumn(name)
        val conv = new java.lang.Integer(value.toString)
        binop match {
          case eq: org.opengis.filter.PropertyIsEqualTo =>
            Option(FilterApi.eq(col, conv))
          case neq: org.opengis.filter.PropertyIsNotEqualTo =>
            Option(FilterApi.notEq(col, conv))
          case lt: org.opengis.filter.PropertyIsLessThan =>
            Option(FilterApi.lt(col, conv))
          case lte: org.opengis.filter.PropertyIsLessThanOrEqualTo =>
            Option(FilterApi.ltEq(col, conv))
          case gt: org.opengis.filter.PropertyIsGreaterThan =>
            Option(FilterApi.gt(col, conv))
          case gte: org.opengis.filter.PropertyIsGreaterThanOrEqualTo =>
            Option(FilterApi.gtEq(col, conv))
          case _ =>
            None
        }

      case ObjectType.DOUBLE =>
        val col = FilterApi.doubleColumn(name)
        val conv = new java.lang.Double(value.toString)
        binop match {
          case eq: org.opengis.filter.PropertyIsEqualTo =>
            Option(FilterApi.eq(col, conv))
          case neq: org.opengis.filter.PropertyIsNotEqualTo =>
            Option(FilterApi.notEq(col, conv))
          case lt: org.opengis.filter.PropertyIsLessThan =>
            Option(FilterApi.lt(col, conv))
          case lte: org.opengis.filter.PropertyIsLessThanOrEqualTo =>
            Option(FilterApi.ltEq(col, conv))
          case gt: org.opengis.filter.PropertyIsGreaterThan =>
            Option(FilterApi.gt(col, conv))
          case gte: org.opengis.filter.PropertyIsGreaterThanOrEqualTo =>
            Option(FilterApi.gtEq(col, conv))
          case _ =>
            None
        }

      case ObjectType.LONG =>
        val col = FilterApi.longColumn(name)
        val conv = new Long(value.toString)
        binop match {
          case eq: org.opengis.filter.PropertyIsEqualTo =>
            Option(FilterApi.eq(col, conv))
          case neq: org.opengis.filter.PropertyIsNotEqualTo =>
            Option(FilterApi.notEq(col, conv))
          case lt: org.opengis.filter.PropertyIsLessThan =>
            Option(FilterApi.lt(col, conv))
          case lte: org.opengis.filter.PropertyIsLessThanOrEqualTo =>
            Option(FilterApi.ltEq(col, conv))
          case gt: org.opengis.filter.PropertyIsGreaterThan =>
            Option(FilterApi.gt(col, conv))
          case gte: org.opengis.filter.PropertyIsGreaterThanOrEqualTo =>
            Option(FilterApi.gtEq(col, conv))
          case _ =>
            None
        }


      case ObjectType.FLOAT =>
        val col = FilterApi.floatColumn(name)
        val conv = new Float(value.toString)
        binop match {
          case eq: org.opengis.filter.PropertyIsEqualTo =>
            Option(FilterApi.eq(col, conv))
          case neq: org.opengis.filter.PropertyIsNotEqualTo =>
            Option(FilterApi.notEq(col, conv))
          case lt: org.opengis.filter.PropertyIsLessThan =>
            Option(FilterApi.lt(col, conv))
          case lte: org.opengis.filter.PropertyIsLessThanOrEqualTo =>
            Option(FilterApi.ltEq(col, conv))
          case gt: org.opengis.filter.PropertyIsGreaterThan =>
            Option(FilterApi.gt(col, conv))
          case gte: org.opengis.filter.PropertyIsGreaterThanOrEqualTo =>
            Option(FilterApi.gtEq(col, conv))
          case _ =>
            None
        }

      case ObjectType.BOOLEAN =>
        val col = FilterApi.booleanColumn(name)
        val conv = new Boolean(value.toString)
        binop match {
          case eq: org.opengis.filter.PropertyIsEqualTo =>
            Option(FilterApi.eq(col, conv))
          case neq: org.opengis.filter.PropertyIsNotEqualTo =>
            Option(FilterApi.notEq(col, conv))
          case _ =>
            None
        }

    }

}
