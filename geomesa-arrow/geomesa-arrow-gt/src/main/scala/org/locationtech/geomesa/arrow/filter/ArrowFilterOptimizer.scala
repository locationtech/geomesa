/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.filter

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Coordinate, Polygon}
import org.geotools.filter.visitor.BindingFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.vector.ArrowAttributeReader.{ArrowDateReader, ArrowLineStringReader, ArrowPointReader}
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, ArrowDictionaryReader, GeometryVector}
import org.locationtech.geomesa.filter.checkOrderUnsafe
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.spatial.BBOX
import org.opengis.filter.temporal.During
import org.opengis.temporal.Period

import scala.util.control.NonFatal

/**
  * Optimizes filters for running against arrow files
  */
object ArrowFilterOptimizer extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConversions._

  private val ff: FilterFactory2 = FastFilterFactory.factory

  def rewrite(filter: Filter, sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Filter = {
    val bound = FastFilterFactory.optimize(sft, filter)
    FastFilterFactory.sfts.set(sft)
    try {
      rewriteFilter(bound, sft, dictionaries)
    } finally {
      FastFilterFactory.sfts.remove()
    }
  }

  private def rewriteFilter(filter: Filter, sft: SimpleFeatureType, dictionaries: Map[String, ArrowDictionary]): Filter = {
    try {
      filter match {
        case f: BBOX              => rewriteBBox(f, sft)
        case f: During            => rewriteDuring(f, sft)
        case f: PropertyIsBetween => rewriteBetween(f, sft)
        case f: PropertyIsEqualTo => rewritePropertyIsEqualTo(f, sft, dictionaries)
        case a: And               => ff.and(a.getChildren.map(rewriteFilter(_, sft, dictionaries)))
        case o: Or                => ff.or(o.getChildren.map(rewriteFilter(_, sft, dictionaries)))
        case f: Not               => ff.not(rewriteFilter(f.getFilter, sft, dictionaries))
        case _                    => filter
      }
    } catch {
      case NonFatal(e) => logger.warn(s"Error re-writing filter $filter", e); filter
    }
  }

  private def rewriteBBox(filter: BBOX, sft: SimpleFeatureType): Filter = {
    if (sft.isPoints || sft.isLines) {
      val props = checkOrderUnsafe(filter.getExpression1, filter.getExpression2)
      val bbox = props.literal.evaluate(null).asInstanceOf[Polygon].getEnvelopeInternal
      val attrIndex = sft.indexOf(props.name)
      if (sft.isPoints) {
        ArrowPointBBox(attrIndex, bbox.getMinX, bbox.getMinY, bbox.getMaxX, bbox.getMaxY)
      } else {
        ArrowLineStringBBox(attrIndex, bbox.getMinX, bbox.getMinY, bbox.getMaxX, bbox.getMaxY)
      }
    } else {
      filter
    }
  }

  private def rewriteDuring(filter: During, sft: SimpleFeatureType): Filter = {
    val props = checkOrderUnsafe(filter.getExpression1, filter.getExpression2)
    val attrIndex = sft.indexOf(props.name)
    val period = props.literal.evaluate(null, classOf[Period])
    val lower = period.getBeginning.getPosition.getDate.getTime
    val upper = period.getEnding.getPosition.getDate.getTime
    ArrowDuring(attrIndex, lower, upper)
  }

  private def rewriteBetween(filter: PropertyIsBetween, sft: SimpleFeatureType): Filter = {
    val attribute = filter.getExpression.asInstanceOf[PropertyName].getPropertyName
    val attrIndex = sft.indexOf(attribute)
    if (sft.getDescriptor(attrIndex).getType.getBinding != classOf[Date]) { filter } else {
      val lower = filter.getLowerBoundary.evaluate(null, classOf[Date]).getTime
      val upper = filter.getUpperBoundary.evaluate(null, classOf[Date]).getTime
      ArrowBetweenDate(attrIndex, lower, upper)
    }
  }

  private def rewritePropertyIsEqualTo(filter: PropertyIsEqualTo,
                                       sft: SimpleFeatureType,
                                       dictionaries: Map[String, ArrowDictionary]): Filter = {
    val props = checkOrderUnsafe(filter.getExpression1, filter.getExpression2)
    dictionaries.get(props.name) match {
      case None => filter
      case Some(dictionary) =>
        val attrIndex = sft.indexOf(props.name)
        val numericValue = dictionary.index(props.literal.evaluate(null))
        ArrowDictionaryEquals(attrIndex, numericValue)
    }
  }

  case class ArrowPointBBox(i: Int, xmin: Double, ymin: Double, xmax: Double, ymax: Double) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean = {
      val arrow = o.asInstanceOf[ArrowSimpleFeature]
      val reader = arrow.getReader(i).asInstanceOf[ArrowPointReader]
      val index = arrow.getIndex
      val y = reader.readPointY(index)
      if (y < ymin || y > ymax) { false } else {
        val x = reader.readPointX(index)
        x >= xmin && x <= xmax
      }
    }
  }

  case class ArrowLineStringBBox(i: Int, xmin: Double, ymin: Double, xmax: Double, ymax: Double) extends Filter {
    private val bboxEnvelope = new ReferencedEnvelope(xmin, xmax, ymin, ymax, CRS_EPSG_4326)
    private val bbox = GeometryVector.factory.toGeometry(bboxEnvelope)

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean = {
      val arrow = o.asInstanceOf[ArrowSimpleFeature]
      val reader = arrow.getReader(i).asInstanceOf[ArrowLineStringReader]
      val (start, end) = reader.readOffsets(arrow.getIndex)
      var offset = start
      val points = Array.ofDim[Coordinate](2)
      while (offset < end) {
        val y = reader.readPointY(offset)
        if (y >= ymin && y <= ymax) {
          val x = reader.readPointX(offset)
          if (x >= xmin && x <= xmax) {
            // we have a point in the bbox, short-circuit and return
            return true
          }
          // check for intersection even if the points aren't contained in the bbox
          points(1) = points(0)
          points(0) = new Coordinate(x, y)
          if (offset > start) {
            val line = GeometryVector.factory.createLineString(points)
            if (line.getEnvelopeInternal.intersects(bboxEnvelope) && line.intersects(bbox)) {
              // found an intersection, short-circuit and return
              return true
            }
          }
        }
        offset += 1
      }
      false
    }
  }

  case class ArrowDuring(i: Int, lower: Long, upper: Long) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean = {
      val arrow = o.asInstanceOf[ArrowSimpleFeature]
      val time = arrow.getReader(i).asInstanceOf[ArrowDateReader].getTime(arrow.getIndex)
      // note that during is exclusive
      time > lower && time < upper
    }
  }

  case class ArrowBetweenDate(i: Int, lower: Long, upper: Long) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean = {
      val arrow = o.asInstanceOf[ArrowSimpleFeature]
      val time = arrow.getReader(i).asInstanceOf[ArrowDateReader].getTime(arrow.getIndex)
      // note that between is inclusive
      time >= lower && time <= upper
    }
  }

  case class ArrowDictionaryEquals(i: Int, value: Int) extends Filter {
    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = extraData
    override def evaluate(o: AnyRef): Boolean = {
      val arrow = o.asInstanceOf[ArrowSimpleFeature]
      arrow.getReader(i).asInstanceOf[ArrowDictionaryReader].getEncoded(arrow.getIndex) == value
    }
  }
}
