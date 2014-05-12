/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.core.data

import collection.JavaConversions._
import com.vividsolutions.jts.geom._
import geomesa.core.index
import geomesa.utils.geotools.Conversions._
import geomesa.utils.geotools.GeometryUtils
import geomesa.utils.text.WKTUtils
import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.visitor.SimplifyingFilterVisitor
import org.geotools.geometry.jts.{JTSFactoryFinder, JTS}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTimeZone, DateTime, Interval}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression._
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._
import org.opengis.temporal.Instant

object FilterToAccumulo {
  val allTime              = new Interval(0, Long.MaxValue)
  val wholeWorld           = new Envelope(-180, 180, -90, 90)

  val noPolygon : Polygon  = null
  val noInterval: Interval = null

  val ff = CommonFactoryFinder.getFilterFactory2
  val geoFactory = JTSFactoryFinder.getGeometryFactory

  implicit class RichPolygon(self: Polygon) {
    def getSafeUnion(other: Polygon): Polygon = {
      if (self != noPolygon && other != noPolygon) {
        if (self.overlaps(other)) {
          val p = self.union(other)
          p.normalize()
          p.asInstanceOf[Polygon]
        } else {
          // they don't overlap; take the merge of their envelopes
          // (since we don't support MultiPolygon returns yet)
          val env = self.getEnvelopeInternal
          env.expandToInclude(other.getEnvelopeInternal)
          JTS.toGeometry(env)
        }
      } else noPolygon
    }
    
    def getSafeIntersection(other: Polygon): Polygon =
      if (self == noPolygon) other
      else if (other == noPolygon) self
      else if (self.intersects(other)) {
        val p = self.intersection(other)
        p.normalize()
        p.asInstanceOf[Polygon]
      } else noPolygon
  }

  implicit class RichInterval(self: Interval) {
    def getSafeUnion(other: Interval): Interval = {
      if (self != noInterval && other != noInterval) {
        new Interval(
          if (self.getStart.isBefore(other.getStart)) self.getStart else other.getStart,
          if (self.getEnd.isAfter(other.getEnd)) self.getEnd else other.getEnd
        )
      } else noInterval
    }

    def getSafeIntersection(other: Interval): Interval =
      if (self == noInterval) other
      else if (other == noInterval) self
      else {
        new Interval(
          if (self.getStart.isBefore(other.getStart)) other.getStart else self.getStart,
          if (self.getEnd.isAfter(other.getEnd)) other.getEnd else self.getEnd
        )
      }
  }
}

import FilterToAccumulo._

// FilterToAccumulo extracts the spatial and temporal predicates from the
// filter while rewriting the filter to optimize for scanning Accumulo
class FilterToAccumulo(sft: SimpleFeatureType) {
  val dtgField  = index.getDtgDescriptor(sft)
  val geomField = sft.getGeometryDescriptor

  var spatialPredicate:  Polygon  = noPolygon
  var temporalPredicate: Interval = noInterval

  def visit(query: Query): Filter = visit(query.getFilter)
  def visit(filter: Filter): Filter =
    process(filter).accept(new SimplifyingFilterVisitor, null).asInstanceOf[Filter]

  case class VisitedData(raw: Filter, evaluated: Filter, polygon: Polygon, interval: Interval) {
    def simplify: Filter = evaluated match {
      case Filter.INCLUDE if polygon != noPolygon && interval != noInterval =>
        ff.and(intersects, during)
      case Filter.INCLUDE if polygon != noPolygon  =>
        intersects
      case Filter.INCLUDE if interval != noInterval =>
        during
      case f => f
    }

    def intersects: Filter = raw match {
      case f: SpatialOperator => f
      case _                  =>
        ff.intersects(ff.property(geomField.getLocalName), ff.literal(polygon))
    }

    def during: Filter = raw match {
      case f: BinaryTemporalOperator => f
      case _                         =>
        ff.during(ff.property(dtgField.getLocalName), ff.literal(interval))
    }
  }

  def simplifyChildren(nodes: Seq[VisitedData], fnx: (Filter, Filter) => Filter): Filter = {
    nodes.map(_.evaluated).filter(_ != Filter.INCLUDE) match {
      case children if children.size == 0 => Filter.INCLUDE
      case children if children.size == 1 => children.head
      case children =>
        children.reduce { (left, right) => ff.or(left, right) }
    }
  }

  def evaluateChildrenIndependently(filter: BinaryLogicOperator): Seq[VisitedData] =
    filter.getChildren.map { child =>
      val f2a = new FilterToAccumulo(sft)
      val childEval = f2a.process(child)
      VisitedData(child, childEval, f2a.spatialPredicate, f2a.temporalPredicate)
    }

  def processOrChildren(op: BinaryLogicOperator): Filter = {
    val nodes = evaluateChildrenIndependently(op)

    spatialPredicate = noPolygon
    temporalPredicate = noInterval

    // you can reduce a sequence of nodes if they all set (only) geometry
    val onlyPolygons = nodes.filter(node => node.polygon != noPolygon && node.interval == noInterval)
    if (onlyPolygons.size == nodes.size) {
      spatialPredicate = nodes.map(_.polygon).reduce { (left, right) =>
        left.getSafeUnion(right) }
      simplifyChildren(nodes, ff.or)
    } else {
      // you can reduce a sequence of nodes if they all set (only) time
      val onlyIntervals = nodes.filter(node => node.polygon == noPolygon && node.interval != noInterval)
      if (onlyIntervals.size == nodes.size) {
        temporalPredicate = nodes.map(_.interval).reduce { (left, right) =>
          left.getSafeUnion(right) }
        simplifyChildren(nodes, ff.or)
      } else {
        // this was neither all geometry nor all interval
        val leftovers = nodes.map(_.simplify).filter(_ != Filter.INCLUDE)
        leftovers.size match {
          case 0 => Filter.INCLUDE
          case 1 => leftovers.head
          case _ => ff.or(leftovers)
        }
      }
    }
  }

  def processAndChildren(op: BinaryLogicOperator): Filter = {
    val nodes = evaluateChildrenIndependently(op)
    val result = nodes.tail.foldLeft(nodes.head)((soFar, node) => {
      val nextEval = (soFar.evaluated, node.evaluated) match {
        case (Filter.EXCLUDE, _) => Filter.EXCLUDE
        case (_, Filter.EXCLUDE) => Filter.EXCLUDE
        case (Filter.INCLUDE, e) => e
        case (e, Filter.INCLUDE) => e
        case (e0, e1)            => ff.and(e0, e1)
      }
      VisitedData(
        Filter.INCLUDE,
        nextEval,
        soFar.polygon.getSafeIntersection(node.polygon),
        soFar.interval.getSafeIntersection(node.interval)
      )
    })

    spatialPredicate = result.polygon
    temporalPredicate = result.interval
    result.evaluated
  }

  def processNot(op: Not): Filter = {
    spatialPredicate = noPolygon
    temporalPredicate = noInterval

    op.getFilter match {
      case f: Not => process(f.getFilter)
      case f: And =>
        process(ff.or(f.getChildren.map(child => ff.not(child))))
      case f: Or  =>
        process(ff.and(f.getChildren.map(child => ff.not(child))))
      case f      =>
        val childEval = process(f)
        val notAttributes =
          if (spatialPredicate != noPolygon || temporalPredicate != noInterval)
            Filter.INCLUDE
          else childEval match {
            case Filter.INCLUDE => Filter.EXCLUDE
            case Filter.EXCLUDE => Filter.INCLUDE
            case f              => ff.not(f)
          }
        val notSpatial = if (spatialPredicate != noPolygon) {
          JTS.toGeometry(wholeWorld).difference(spatialPredicate) match {
            case p: Polygon =>
              spatialPredicate = p
              Filter.INCLUDE
            case _          =>
              val oldSpace = spatialPredicate
              spatialPredicate = noPolygon
              ff.not(ff.intersects(ff.property(geomField.getLocalName), ff.literal(oldSpace)))
          }
        } else Filter.INCLUDE
        val notTemporal = if (temporalPredicate != noInterval) {
          val oldTemporal = temporalPredicate
          temporalPredicate = noInterval
          ff.not(ff.during(ff.property(dtgField.getLocalName), ff.literal(oldTemporal)))
        } else Filter.INCLUDE
        // these three components are joined by an implied AND; build (and simplify) that expression
        Seq[Filter](notSpatial, notTemporal, notAttributes).foldLeft(Filter.INCLUDE.asInstanceOf[Filter])((filterSoFar, subFilter) =>
          (filterSoFar, subFilter) match {
            case (Filter.EXCLUDE, s) => Filter.EXCLUDE
            case (f, Filter.EXCLUDE) => Filter.EXCLUDE
            case (Filter.INCLUDE, s) => s
            case (f, Filter.INCLUDE) => f
            case (f, s) => ff.and(f, s)
          })
    }
  }

  def process(filter: Filter, acc: Filter = Filter.INCLUDE): Filter = filter match {
    // Logical filters
    case op: Or    => processOrChildren(op)
    case op: And   => processAndChildren(op)

    // Negation filter
    case op: Not   => processNot(op)

    // Spatial filters
    case op: BBOX       => visitBBOX(op, acc)
    case op: DWithin    => visitDWithin(op, acc)
    case op: Within     => visitBinarySpatialOp(op, acc)
    case op: Intersects => visitBinarySpatialOp(op, acc)
    case op: Overlaps   => visitBinarySpatialOp(op, acc)

    // Temporal filters
    case op: BinaryTemporalOperator => visitBinaryTemporalOp(op, acc)

    // Other
    case op: PropertyIsBetween      => visitPropertyIsBetween(op, acc)

    // Catch all
    case f: Filter => ff.and(acc, f)
  }

  private def visitBBOX(op: BBOX, acc: Filter): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val attr = e1.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(sft.getGeometryDescriptor.getLocalName)) {
      ff.and(acc, op)
    } else {
      spatialPredicate = JTS.toGeometry(op.getBounds)
      acc
    }
  }

  private def visitBinarySpatialOp(op: BinarySpatialOperator, acc: Filter): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val attr = e1.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(sft.getGeometryDescriptor.getLocalName)) {
      ff.and(acc, op)
    } else {
      val geom = e2.evaluate(null, classOf[Geometry])
      spatialPredicate = geom.asInstanceOf[Polygon]
      if(!geom.isRectangle) ff.and(acc, op)
      else acc
    }
  }

  def visitDWithin(op: DWithin, acc: Filter): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val attr = e1.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(sft.getGeometryDescriptor.getLocalName)) {
      ff.and(acc, op)
    } else {
      val geoCalc = new GeodeticCalculator(DefaultGeographicCRS.WGS84)
      val startPoint = e2.evaluate(null, classOf[Point])
      val distance = op.getDistance

      // Convert meters to dec degrees based on widest point in dec degrees of circle
      geoCalc.setStartingGeographicPoint(startPoint.getX, startPoint.getY)
      geoCalc.setDirection(90, distance)
      val right = geoCalc.getDestinationGeographicPoint
      val distanceDegrees = startPoint.distance(geoFactory.createPoint(new Coordinate(right.getX, right.getY)))

      // Walk circle bounds for bounding box
      spatialPredicate = GeometryUtils.bufferPoint(startPoint, distance)

      val rewrittenFilter =
        ff.dwithin(
          ff.property(sft.getGeometryDescriptor.getLocalName),
          ff.literal(startPoint),
          distanceDegrees,
          "meters")
      ff.and(acc, rewrittenFilter)
    }
  }

  def visitBinaryTemporalOp(bto: BinaryTemporalOperator, acc: Filter): Filter = {
    val prop     = bto.getExpression1.asInstanceOf[PropertyName]
    val lit      = bto.getExpression2.asInstanceOf[Literal]
    val attr     = prop.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(dtgField.getLocalName)) ff.and(acc, bto)
    else {
      val period = lit.evaluate(null).asInstanceOf[org.opengis.temporal.Period]
      temporalPredicate = bto match {
        case op: Before    => new Interval(new DateTime(0L), period.getEnding)
        case op: After     => new Interval(period.getBeginning, new DateTime(Long.MaxValue))
        case op: During    => new Interval(period.getBeginning, period.getEnding)
        case op: TContains => new Interval(period.getBeginning, period.getEnding)
        case _             => throw new IllegalArgumentException("Invalid query")
      }
      acc
    }
  }

  def visitPropertyIsBetween(op: PropertyIsBetween, acc: Filter): Filter = {
    val prop = op.getExpression.asInstanceOf[PropertyName]
    val attr = prop.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(dtgField.getLocalName)) ff.and(acc, op)
    else {
      val start = extractDTG(op.getLowerBoundary.evaluate(null))
      val end   = extractDTG(op.getUpperBoundary.evaluate(null))
      temporalPredicate = new Interval(start, end)
      acc
    }
  }

  private def extractDTG(o: AnyRef) = parseDTG(o).withZone(DateTimeZone.UTC)

  private def parseDTG(o: AnyRef): DateTime = o match {
    case i:  Instant                => new DateTime(i.getPosition.getDate.getTime)
    case d:  java.util.Date         => new DateTime(d.getTime)
    case j:  org.joda.time.Instant  => j.toDateTime
    case dt: DateTime               => dt
    case s:  String                 => ISODateTimeFormat.dateTime().parseDateTime(s)
    case _                          => throw new IllegalArgumentException("Unknown dtg type")
  }

}
