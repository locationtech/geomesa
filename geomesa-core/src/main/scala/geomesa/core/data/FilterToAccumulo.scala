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

import com.vividsolutions.jts.geom._
import geomesa.core.data.FilterToAccumulo._
import geomesa.core.index
import geomesa.utils.filters.Filters._
import geomesa.utils.geohash.GeohashUtils.getInternationalDateLineSafeGeometry
import geomesa.utils.geometry.Geometry._
import geomesa.utils.geotools.Conversions._
import geomesa.utils.geotools.GeometryUtils
import geomesa.utils.time.Time._
import org.geotools.data.Query
import org.geotools.filter.visitor.SimplifyingFilterVisitor
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPosition}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression._
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._
import org.opengis.temporal.{Instant, Period => OGCPeriod}
import scala.collection.JavaConversions._

object FilterToAccumulo {
  val allTime              = new Interval(0, Long.MaxValue)
  val wholeWorld           = new Envelope(-180, 180, -90, 90)

  val geoFactory = JTSFactoryFinder.getGeometryFactory

  val MinTime = new DateTime(0L)
  val MaxTime = new DateTime(Long.MaxValue)

  val DTF = ISODateTimeFormat.dateTime
}

// FilterToAccumulo extracts the spatial and temporal predicates from the
// filter while rewriting the filter to optimize for scanning Accumulo
class FilterToAccumulo(sft: SimpleFeatureType) {
  // if the dtg attribute doesn't exist, set the name to an empty string
  val dtgFieldName  = index.getDtgDescriptor(sft).map{_.getLocalName}.getOrElse("")
  val geomFieldName = sft.getGeometryDescriptor.getLocalName

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
        ff.intersects(ff.property(geomFieldName), ff.literal(polygon))
    }

    def during: Filter = raw match {
      case f: BinaryTemporalOperator => f
      case _                         =>
        ff.during(ff.property(dtgFieldName), interval2lit(interval))
    }
  }

  def simplifyChildren(nodes: Seq[VisitedData], fnx: (Filter, Filter) => Filter): Filter = {
    nodes.map(_.evaluated).filter(_ != Filter.INCLUDE) match {
      case children if children.size == 0 => Filter.INCLUDE
      case children if children.size == 1 => children.head
      case children =>  ff.or(children)
    }
  }

  def evaluateChildrenIndependently(filter: BinaryLogicOperator): Seq[VisitedData] =
    filter.getChildren.map { child =>
      val f2a = new FilterToAccumulo(sft)
      val childEval = f2a.process(child)
      VisitedData(child, childEval, f2a.spatialPredicate, f2a.temporalPredicate)
    }

  def onlyPolygons(nodes: Seq[VisitedData]) = !nodes.exists {
    case node if node.polygon == noPolygon   => true
    case node if node.interval != noInterval => true
    case _                                   => false
  }

  def onlyIntervals(nodes: Seq[VisitedData]) = !nodes.exists {
    case node if node.polygon != noPolygon   => true
    case node if node.interval == noInterval => true
    case _                                   => false
  }

  def processOrChildren(op: BinaryLogicOperator): Filter = {
    val nodes = evaluateChildrenIndependently(op)

    spatialPredicate = noPolygon
    temporalPredicate = noInterval

    // you can reduce a sequence of nodes if they all set (only) geometry
    if (onlyPolygons(nodes)) {
      spatialPredicate = nodes.map(_.polygon).reduce { (left, right) =>
        left.getSafeUnion(right) }
      simplifyChildren(nodes, ff.or)
    } else {
      // you can reduce a sequence of nodes if they all set (only) time
      if (onlyIntervals(nodes)) {
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

  def negateSingleton(childEval: Filter): Filter = {
    val notAttributes = (spatialPredicate, temporalPredicate, childEval) match {
      case (sp, tp, _) if sp != noPolygon || tp != noInterval => Filter.INCLUDE
      case (_, _, Filter.INCLUDE)                             => Filter.EXCLUDE
      case (_, _, Filter.EXCLUDE)                             => Filter.INCLUDE
      case (_, _, f)                                          => ff.not(f)
    }
    val notSpatial = if (spatialPredicate != noPolygon) {
      JTS.toGeometry(wholeWorld).difference(spatialPredicate) match {
        case p: Polygon =>
          spatialPredicate = p
          Filter.INCLUDE
        case _          =>
          val oldSpace = spatialPredicate
          spatialPredicate = noPolygon
          ff.not(ff.intersects(ff.property(geomFieldName), ff.literal(oldSpace)))
      }
    } else Filter.INCLUDE

    val notTemporal = if (temporalPredicate != noInterval) {
      val oldTemporal = temporalPredicate
      temporalPredicate = noInterval
      (oldTemporal.getStart, oldTemporal.getEnd) match {
        case (MinTime, MaxTime)  => Filter.EXCLUDE
        case (s, MaxTime)        =>
          ff.not(ff.after(ff.property(dtgFieldName), dt2lit(s)))
        case (MinTime, e)        =>
          ff.not(ff.before(ff.property(dtgFieldName), dt2lit(e)))
        case (s, e)              =>
          ff.not(ff.during(ff.property(dtgFieldName), dts2lit(s, e)))
      }
    } else Filter.INCLUDE

    // these three components are joined by an implied AND; build (and simplify) that expression
    Seq[Filter](notSpatial, notTemporal, notAttributes).foldLeft(Filter.INCLUDE.asInstanceOf[Filter])((filterSoFar, subFilter) =>
      (filterSoFar, subFilter) match {
        case (Filter.EXCLUDE, _) => Filter.EXCLUDE
        case (_, Filter.EXCLUDE) => Filter.EXCLUDE
        case (Filter.INCLUDE, s) => s
        case (f, Filter.INCLUDE) => f
        case (f, s) => ff.and(f, s)
      })
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
      case f      => negateSingleton(process(f))
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
      val e2 = op.getExpression2.asInstanceOf[Literal]
      val geom = addWayPointsToBBOX( e2.evaluate(null, classOf[Geometry]) )
      val safeGeometry = getInternationalDateLineSafeGeometry(geom)
      spatialPredicate = safeGeometry.getEnvelope.asInstanceOf[Polygon]
      updateToIDLSafeFilter(op, acc, safeGeometry)
    }
  }

  private def visitBinarySpatialOp(op: BinarySpatialOperator, acc: Filter): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val attr = e1.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(sft.getGeometryDescriptor.getLocalName)) {
      ff.and(acc, op)
    } else {
      val e2 = op.getExpression2.asInstanceOf[Literal]
      val geom = e2.evaluate(null, classOf[Geometry])
      val safeGeometry = getInternationalDateLineSafeGeometry(geom)
      spatialPredicate = safeGeometry.getEnvelope.asInstanceOf[Polygon]
      updateToIDLSafeFilter(op, acc, safeGeometry)
    }
  }

  def visitDWithin(op: DWithin, acc: Filter): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val attr = e1.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(sft.getGeometryDescriptor.getLocalName)) {
      ff.and(acc, op)
    } else {
      val e2 = op.getExpression2.asInstanceOf[Literal]
      val startPoint = e2.evaluate(null, classOf[Point])
      val distance = op.getDistance
      val distanceDegrees = GeometryUtils.distanceDegrees(startPoint, distance)

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
    if(!attr.getLocalName.equals(dtgFieldName)) ff.and(acc, bto)
    else {
      val time = lit.evaluate(null)
      val startTime = getStart(time)
      val endTime = getEnd(time)
      temporalPredicate = bto match {
        case op: Before    => new Interval(MinTime, endTime)
        case op: After     => new Interval(startTime, MaxTime)
        case op: During    => new Interval(startTime, endTime)
        case op: TContains => new Interval(startTime, endTime)
        case _             => throw new IllegalArgumentException("Invalid query")
      }
      acc
    }
  }

  def visitPropertyIsBetween(op: PropertyIsBetween, acc: Filter): Filter = {
    val prop = op.getExpression.asInstanceOf[PropertyName]
    val attr = prop.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(dtgFieldName)) ff.and(acc, op)
    else {
      val start = extractDTG(op.getLowerBoundary.evaluate(null))
      val end   = extractDTG(op.getUpperBoundary.evaluate(null))
      temporalPredicate = new Interval(start, end)
      acc
    }
  }

  def updateToIDLSafeFilter(op: BinarySpatialOperator, acc: Filter, geom: Geometry) = geom match {
    case p: Polygon =>
      if (!geom.isRectangle) ff.and(acc, op)
      else acc
    case mp: MultiPolygon =>
      val polygonList = getGeometryListOf(geom)
      val filterList = polygonList.map {
        p => doCorrectSpatialCall(op, sft.getGeometryDescriptor.getLocalName, p)
      }
      ff.and(acc, ff.or(filterList))
  }

  def addWayPointsToBBOX(g: Geometry):Geometry = {
    val gf = g.getFactory
    val geomArray = g.getCoordinates
    val correctedGeom = GeometryUtils.addWayPoints(geomArray).toArray
    gf.createPolygon(correctedGeom)
  }

  def doCorrectSpatialCall(op: BinarySpatialOperator, property: String, geom: Geometry): Filter = op match {
    case op: Within     => ff.within( ff.property(property), ff.literal(geom) )
    case op: Intersects => ff.intersects( ff.property(property), ff.literal(geom) )
    case op: Overlaps   => ff.overlaps( ff.property(property), ff.literal(geom) )
    case op: BBOX       => val envelope = geom.getEnvelopeInternal
                           ff.bbox( ff.property(property), envelope.getMinX, envelope.getMinY,
                           envelope.getMaxX, envelope.getMaxY, op.getSRS )
  }

  def getGeometryListOf(inMP: Geometry): Seq[Geometry] =
    for( i <- 0 until inMP.getNumGeometries ) yield inMP.getGeometryN(i)

  private def extractDTG(o: AnyRef) = parseDTG(o).withZone(DateTimeZone.UTC)

  private def parseDTG(o: AnyRef): DateTime = o match {
    case i:  Instant                => new DateTime(i.getPosition.getDate.getTime)
    case d:  java.util.Date         => new DateTime(d.getTime)
    case j:  org.joda.time.Instant  => j.toDateTime
    case dt: DateTime               => dt
    case s:  String                 => ISODateTimeFormat.dateTime().parseDateTime(s)
    case _                          => throw new IllegalArgumentException("Unknown dtg type")
  }

  private def getStart(o: AnyRef): Instant = o match {
    case p: OGCPeriod => p.getBeginning
    case _            => new DefaultInstant(
      new DefaultPosition(extractDTG(o).toDate))
  }

  private def getEnd(o: AnyRef): Instant = o match {
    case p: OGCPeriod => p.getEnding
    case _            => new DefaultInstant(
      new DefaultPosition(extractDTG(o).toDate))
  }
}