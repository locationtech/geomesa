/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.index

import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, MultiPolygon, Polygon}
import org.joda.time.{DateTime, Interval}
import org.locationtech.geomesa.accumulo.filter._
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geohash.GeohashUtils._
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Expression, Literal, PropertyName}
import org.opengis.filter.spatial._
import org.opengis.filter.temporal.{After, Before, During}
import org.opengis.temporal.Period

import scala.collection.JavaConversions._

object FilterHelper {
  // Let's handle special cases with topological filters.
  def updateTopologicalFilters(filter: Filter, featureType: SimpleFeatureType) = {
    filter match {
      case dw: DWithin    => rewriteDwithin(dw)
      case op: BBOX       => visitBBOX(op, featureType)
      case op: Within     => visitBinarySpatialOp(op, featureType)
      case op: Intersects => visitBinarySpatialOp(op, featureType)
      case op: Overlaps   => visitBinarySpatialOp(op, featureType)
      case _ => filter
    }
  }

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

  def isWholeWorld[G <: Geometry](g: G): Boolean = g != null && g.union.covers(IndexSchema.everywhere)

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
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val geom = e2.getValue.asInstanceOf[Geometry]
    val distanceDegrees = GeometryUtils.distanceDegrees(geom, op.getDistance)

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
  def extractTemporal(dtFieldName: Option[String]): Seq[Filter] => Interval = {
    import org.locationtech.geomesa.utils.filters.Typeclasses.BinaryFilter
    import org.locationtech.geomesa.utils.filters.Typeclasses.BinaryFilter.ops

    def endpointFromBinaryFilter[B: BinaryFilter](b: B, dtfn: String) = {
      val exprToDT: Expression => DateTime = ex => new DateTime(ex.evaluate(null, classOf[Date]))
      if (b.left.toString == dtfn) {
        Right(exprToDT(b.right))  // the left side is the field name; the right is the endpoint
      } else {
        Left(exprToDT(b.left))    // the right side is the field name; the left is the endpoint
      }
    }

    def intervalFromAfterLike[B: BinaryFilter](b: B, dtfn: String) =
      endpointFromBinaryFilter(b, dtfn) match {
        case Right(dt) => new Interval(dt, IndexSchema.maxDateTime)
        case Left(dt)  => new Interval(IndexSchema.minDateTime, dt)
      }

    def intervalFromBeforeLike[B: BinaryFilter](b: B, dtfn: String) =
      endpointFromBinaryFilter(b, dtfn) match {
        case Right(dt) => new Interval(IndexSchema.minDateTime, dt)
        case Left(dt)  => new Interval(dt, IndexSchema.maxDateTime)
      }

    def extractInterval(dtfn: String): Filter => Interval = {
      case during: During =>
        val p = during.getExpression2.evaluate(null, classOf[Period])
        val start = p.getBeginning.getPosition.getDate
        val end = p.getEnding.getPosition.getDate
        new Interval(start.getTime, end.getTime)
      case between: PropertyIsBetween =>
        val start = between.getLowerBoundary.evaluate(null, classOf[Date])
        val end = between.getUpperBoundary.evaluate(null, classOf[Date])
        new Interval(start.getTime, end.getTime)
      // NB: Interval semantics correspond to "at or after"
      case after: After =>                        intervalFromAfterLike(after, dtfn)
      case before: Before =>                      intervalFromBeforeLike(before, dtfn)

      case lt: PropertyIsLessThan =>              intervalFromBeforeLike(lt, dtfn)
      // NB: Interval semantics correspond to <
      case le: PropertyIsLessThanOrEqualTo =>     intervalFromBeforeLike(le, dtfn)
      // NB: Interval semantics correspond to >=
      case gt: PropertyIsGreaterThan =>           intervalFromAfterLike(gt, dtfn)
      case ge: PropertyIsGreaterThanOrEqualTo =>  intervalFromAfterLike(ge, dtfn)
      case a: Any =>
        throw new Exception(s"Expected temporal filters.  Received an $a.")
    }

    dtFieldName match {
      case None =>
        _ => IndexSchema.everywhen
      case Some(dtfn) =>
        filters => filters.map(extractInterval(dtfn)).fold(IndexSchema.everywhen)( _.overlap(_))
    }
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
}

trait IndexFilterHelpers {

  def buildFilter(geom: Geometry, interval: Interval): KeyPlanningFilter =
    (IndexSchema.somewhere(geom), IndexSchema.somewhen(interval)) match {
      case (None, None)       =>    AcceptEverythingFilter
      case (None, Some(i))    =>
        if (i.getStart == i.getEnd) DateFilter(i.getStart)
        else                        DateRangeFilter(i.getStart, i.getEnd)
      case (Some(p), None)    =>    SpatialFilter(p)
      case (Some(p), Some(i)) =>
        if (i.getStart == i.getEnd) SpatialDateFilter(p, i.getStart)
        else                        SpatialDateRangeFilter(p, i.getStart, i.getEnd)
    }

  def netGeom(geom: Geometry): Geometry =
    Option(geom).map(_.intersection(IndexSchema.everywhere)).orNull

  def netInterval(interval: Interval): Interval = interval match {
    case null => null
    case _    => IndexSchema.everywhen.overlap(interval)
  }

  def netPolygon(poly: Polygon): Polygon = poly match {
    case null => null
    case p if p.covers(IndexSchema.everywhere) =>
      IndexSchema.everywhere
    case p if IndexSchema.everywhere.covers(p) => p
    case _ => poly.intersection(IndexSchema.everywhere).
      asInstanceOf[Polygon]
  }

}