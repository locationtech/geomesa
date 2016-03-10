/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter

import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, MultiPolygon, Polygon}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.locationtech.geomesa.filter.visitor.SafeTopologicalFilterVisitorImpl
import org.locationtech.geomesa.utils.filters.Typeclasses.BinaryFilter
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geohash.GeohashUtils._
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Expression, Literal, PropertyName}
import org.opengis.filter.spatial._
import org.opengis.filter.temporal.{After, Before, During, TEquals}
import org.opengis.temporal.Period

import scala.collection.JavaConversions._

object FilterHelper {
  // Let's handle special cases with topological filters.
  def updateTopologicalFilters(filter: Filter, sft: SimpleFeatureType): Filter =
    filter.accept(new SafeTopologicalFilterVisitorImpl(sft), null).asInstanceOf[Filter]

  def visitBinarySpatialOp(op: BinarySpatialOperator, featureType: SimpleFeatureType): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val geom = e2.evaluate(null, classOf[Geometry])
    val safeGeometry = getInternationalDateLineSafeGeometry(geom)
    updateToIDLSafeFilter(op, safeGeometry, featureType)
  }

  def visitBBOX(op: BBOX, featureType: SimpleFeatureType): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val geom = addWayPointsToBBOX( e2.evaluate(null, classOf[Geometry]) )
    val safeGeometry = getInternationalDateLineSafeGeometry(geom)
    updateToIDLSafeFilter(op, safeGeometry, featureType)
  }

  def updateToIDLSafeFilter(op: BinarySpatialOperator, geom: Geometry, featureType: SimpleFeatureType): Filter = geom match {
    case p: Polygon =>
      dispatchOnSpatialType(op, featureType.getGeometryDescriptor.getLocalName, p)
    case mp: MultiPolygon =>
      val polygonList = getGeometryListOf(geom)
      val filterList = polygonList.map {
        p => dispatchOnSpatialType(op, featureType.getGeometryDescriptor.getLocalName, p)
      }
      ff.or(filterList)
  }

  def isFilterWholeWorld(f: Filter): Boolean = f match {
      case op: BBOX       => isOperationGeomWholeWorld(op)
      case op: Within     => isOperationGeomWholeWorld(op)
      case op: Intersects => isOperationGeomWholeWorld(op)
      case op: Overlaps   => isOperationGeomWholeWorld(op)
      case _ => false
    }

  private def isOperationGeomWholeWorld[Op <: BinarySpatialOperator](op: Op): Boolean = {
    val prop = checkOrder(op.getExpression1, op.getExpression2)
    prop.map(_.literal.evaluate(null, classOf[Geometry])).exists(isWholeWorld)
  }

  val minDateTime = new DateTime(0, 1, 1, 0, 0, 0, DateTimeZone.UTC).getMillis
  val maxDateTime = new DateTime(9999, 12, 31, 23, 59, 59, DateTimeZone.UTC).getMillis
  val everywhen = new Interval(minDateTime, maxDateTime, DateTimeZone.UTC)
  val everywhere = WKTUtils.read("POLYGON((-180 -90, 0 -90, 180 -90, 180 90, 0 90, -180 90, -180 -90))").asInstanceOf[Polygon]

  def isWholeWorld[G <: Geometry](g: G): Boolean = g != null && g.union.covers(everywhere)

  def getGeometryListOf(inMP: Geometry): Seq[Geometry] =
    for( i <- 0 until inMP.getNumGeometries ) yield inMP.getGeometryN(i)

  def dispatchOnSpatialType(op: BinarySpatialOperator, property: String, geom: Geometry): Filter = op match {
    case op: Within     => ff.within( ff.property(property), ff.literal(geom) )
    case op: Intersects => ff.intersects( ff.property(property), ff.literal(geom) )
    case op: Overlaps   => ff.overlaps( ff.property(property), ff.literal(geom) )
    case op: BBOX       => val envelope = geom.getEnvelopeInternal
      ff.bbox( ff.property(property), envelope.getMinX, envelope.getMinY,
        envelope.getMaxX, envelope.getMaxY, op.getSRS )
  }

  def addWayPointsToBBOX(g: Geometry): Geometry = {
    val gf = g.getFactory
    val geomArray = g.getCoordinates
    val correctedGeom = GeometryUtils.addWayPoints(geomArray).toArray
    gf.createPolygon(correctedGeom)
  }

  // Rewrites a Dwithin (assumed to express distance in meters) in degrees.
  def rewriteDwithin(op: DWithin): Filter = {
    val geom = op.getExpression2.asInstanceOf[Literal].getValue.asInstanceOf[Geometry]
    val units = Option(op.getDistanceUnits).map(_.trim).filter(_.nonEmpty).map(_.toLowerCase).getOrElse("meters")
    val multiplier = units match {
      case "meters"         => 1.0
      case "kilometers"     => 1000.0
      case "feet"           => 0.3048
      case "statute miles"  => 1609.347
      case "nautical miles" => 1852.0
      case _                => 1.0 // not part of ECQL spec...
    }
    val distanceMeters = op.getDistance * multiplier
    val distanceDegrees = GeometryUtils.distanceDegrees(geom, distanceMeters)

    // NB: The ECQL spec doesn't allow for us to put the measurement in "degrees",
    //  but that's how this filter will be used.
    ff.dwithin(
      op.getExpression1,
      op.getExpression2,
      distanceDegrees,
      "meters")
  }

  def decomposeToGeometry(f: Filter): Seq[Geometry] = f match {
    case bbox: BBOX =>
      val bboxPoly = bbox.getExpression2.asInstanceOf[Literal].evaluate(null, classOf[Geometry])
      Seq(bboxPoly)
    case gf: BinarySpatialOperator =>
      extractGeometry(gf)
    case _ => Seq()
  }

  def extractGeometry(bso: BinarySpatialOperator): Seq[Geometry] = {
    bso match {
      // The Dwithin has already between rewritten.
      case dwithin: DWithin =>
        val e2 = dwithin.getExpression2.asInstanceOf[Literal]
        val geom = e2.getValue.asInstanceOf[Geometry]
        val buffer = dwithin.getDistance
        val bufferedGeom = geom.buffer(buffer)
        Seq(GeohashUtils.getInternationalDateLineSafeGeometry(bufferedGeom))
      case bs =>
        bs.getExpression1.evaluate(null, classOf[Geometry]) match {
          case g: Geometry => Seq(GeohashUtils.getInternationalDateLineSafeGeometry(g))
          case _           =>
            bso.getExpression2.evaluate(null, classOf[Geometry]) match {
              case g: Geometry => Seq(GeohashUtils.getInternationalDateLineSafeGeometry(g))
            }
        }
    }
  }

  // NB: This method assumes that the filters represent a collection of 'and'ed temporal filters.
  def extractInterval(filters: Seq[Filter], dtField: Option[String], exclusive: Boolean = false): Interval =
    dtField match {
      case None      => everywhen
      case Some(dtf) =>
        val intervals = filters.map(extractInterval(_, dtf, exclusive))
        val (s, e) = intervals.fold((minDateTime, maxDateTime))(overlap)
        if (s > e) null else new Interval(s, e, DateTimeZone.UTC)
    }

  private def overlap(dt1: (Long, Long), dt2: (Long, Long)): (Long, Long) =
    (math.max(dt1._1, dt2._1), math.min(dt1._2, dt2._2))

  private def extractInterval(filter: Filter, dtField: String, exclusive: Boolean): (Long, Long) =
    filter match {
      case during: During =>
        val p = during.getExpression2.evaluate(null, classOf[Period])
        val start = new DateTime(p.getBeginning.getPosition.getDate, DateTimeZone.UTC).getMillis
        val end = new DateTime(p.getEnding.getPosition.getDate, DateTimeZone.UTC).getMillis
        if (exclusive) {
          (roundSecondsUp(start), roundSecondsDown(end))
        } else {
          (start, end)
        }

      case between: PropertyIsBetween =>
        val start = between.getLowerBoundary.evaluate(null, classOf[Date])
        val end = between.getUpperBoundary.evaluate(null, classOf[Date])
        (start.getTime, end.getTime)

      case eq: PropertyIsEqualTo => intervalFromEqualsLike(eq, dtField)
      case teq: TEquals          => intervalFromEqualsLike(teq, dtField)

      // NB: Interval semantics correspond to "at or after"
      case after: After   => intervalFromAfterLike(after, dtField, exclusive)
      case before: Before => intervalFromBeforeLike(before, dtField, exclusive)

      case lt: PropertyIsLessThan             => intervalFromBeforeLike(lt, dtField, exclusive)
      // NB: Interval semantics correspond to <
      case le: PropertyIsLessThanOrEqualTo    => intervalFromBeforeLike(le, dtField, exclusive = false)
      // NB: Interval semantics correspond to >=
      case gt: PropertyIsGreaterThan          => intervalFromAfterLike(gt, dtField, exclusive)
      case ge: PropertyIsGreaterThanOrEqualTo => intervalFromAfterLike(ge, dtField, exclusive = false)

      case a: Any => throw new Exception(s"Expected temporal filters, but received $a.")
    }

  private def intervalFromEqualsLike[B: BinaryFilter](b: B, dtField: String) =
    endpointFromBinaryFilter(b, dtField) match {
      case Right(dt) => (dt, dt)
      case Left(dt)  => (dt, dt)
    }

  private def intervalFromAfterLike[B: BinaryFilter](b: B, dtField: String, exclusive: Boolean) =
    endpointFromBinaryFilter(b, dtField) match {
      case Right(dt) =>
        if (exclusive) (roundSecondsUp(dt), maxDateTime) else (dt, maxDateTime)
      case Left(dt)  =>
        if (exclusive) (minDateTime, roundSecondsDown(dt)) else (minDateTime, dt)
    }

  def intervalFromBeforeLike[B: BinaryFilter](b: B, dtField: String, exclusive: Boolean) =
    endpointFromBinaryFilter(b, dtField) match {
      case Right(dt) =>
        if (exclusive) (minDateTime, roundSecondsDown(dt)) else (minDateTime, dt)
      case Left(dt)  =>
        if (exclusive) (roundSecondsUp(dt), maxDateTime) else (dt, maxDateTime)
    }

  private def endpointFromBinaryFilter[B: BinaryFilter](b: B, dtField: String) = {
    import org.locationtech.geomesa.utils.filters.Typeclasses.BinaryFilter.ops
    val exprToDT: Expression => Long = ex => ex.evaluate(null, classOf[Date]).getTime
    if (b.left.toString == dtField) {
      Right(exprToDT(b.right))  // the left side is the field name; the right is the endpoint
    } else {
      Left(exprToDT(b.left))    // the right side is the field name; the left is the endpoint
    }
  }

  private def roundSecondsUp(t: Long): Long = t - new DateTime(t).getMillisOfSecond + 1000

  private def roundSecondsDown(t: Long): Long = {
    val millis = new DateTime(t).getMillisOfSecond
    if (millis == 0) t - 1000 else t - millis
  }

  def filterListAsAnd(filters: Seq[Filter]): Option[Filter] = filters match {
    case Nil => None
    case _ => Some(recomposeAnd(filters))
  }

  def recomposeAnd(s: Seq[Filter]): Filter = if (s.tail.isEmpty) s.head else ff.and(s)

  /**
   * Finds the first filter satisfying the condition and returns the rest in the same order they were in
   */
  def findFirst(pred: Filter => Boolean)(s: Seq[Filter]): (Option[Filter], Seq[Filter]) =
    if (s.isEmpty) (None, s) else {
      val h = s.head
      val t = s.tail
      if (pred(h)) (Some(h), t) else {
        val (x, xs) = findFirst(pred)(t)
        (x, h +: xs)
      }
    }

  /**
   * Finds the filter with the lowest known cost and returns the rest in the same order they were in.  If
   * there are multiple filters with the same lowest cost then the first will be selected.  If no filters
   * have a known cost or if ``s`` is empty then (None, s) will be returned.
   */
  def findBest(cost: Filter => Option[Long])(s: Seq[Filter]): CostAnalysis = {
    if (s.isEmpty) {
      CostAnalysis.unknown(s)
    } else {
      val head = s.head
      val tail = s.tail

      val headAnalysis = cost(head).map(c => new KnownCost(head, tail, c)).getOrElse(CostAnalysis.unknown(s))
      val tailAnalysis = findBest(cost)(tail)

      if (headAnalysis <= tailAnalysis) {
        headAnalysis
      } else {
        // tailAnaysis must have a known cost
        val ta = tailAnalysis.asInstanceOf[KnownCost]
        new KnownCost(ta.best, head +: ta.otherFilters, ta.cost)
      }
    }
  }

  /**
    * @param bestFilter the [[Filter]] with the lowest cost
    * @param otherFilters all other [[Filter]]s
    */
  sealed abstract case class CostAnalysis(bestFilter: Option[Filter], otherFilters: Seq[Filter]) {

    /**
      * @param rhs the [[CostAnalysis]] to compare to
      * @return ``true`` if ``this`` has a lower or the same cost as ``rhs``
      */
    def <=(rhs: CostAnalysis): Boolean

    def extract: (Option[Filter], Seq[Filter]) = (bestFilter, otherFilters)
  }

  class KnownCost(val best: Filter, others: Seq[Filter], val cost: Long) extends CostAnalysis(Some(best), others) {

    def <=(rhs: CostAnalysis): Boolean = rhs match {
      case knownRhs: KnownCost =>
        this.cost <= knownRhs.cost
      case _ =>
        // always less than an unknown cost
        true
    }
  }

  class UnknownCost(filters: Seq[Filter]) extends CostAnalysis(None, filters) {
    override def <=(rhs: CostAnalysis): Boolean = rhs.isInstanceOf[UnknownCost]
  }

  object CostAnalysis {

    /**
      * @param filters the filters, none of which have a known cost
      * @return an [[UnknownCost]] containing ``filters``
      */
    def unknown(filters: Seq[Filter]): CostAnalysis = new UnknownCost(filters)
  }

  def decomposeAnd(f: Filter): Seq[Filter] = {
    f match {
      case b: And => b.getChildren.toSeq.flatMap(decomposeAnd)
      case f: Filter => Seq(f)
    }
  }

  def tryReduceGeometryFilter(filts: Seq[Filter]): Seq[Filter] = {
    import org.geotools.data.DataUtilities._
    import scala.collection.JavaConversions._

    val filtFactory = CommonFactoryFinder.getFilterFactory2

    def getAttrName(l: BBOX): String = propertyNames(l).head.getPropertyName

    filts match {
      // if we have two bbox filters as is common in WMS queries, merge them by intersecting the bounds
      case Seq(l: BBOX, r: BBOX) if getAttrName(l) == getAttrName(r) =>
        val prop = propertyNames(l).head
        val bounds = JTS.toGeometry(l.getBounds).intersection(JTS.toGeometry(r.getBounds)).getEnvelopeInternal
        val re = ReferencedEnvelope.reference(bounds)
        val bbox = filtFactory.bbox(prop, re)
        Seq(bbox)

      case _ =>
        filts
    }
  }

}

