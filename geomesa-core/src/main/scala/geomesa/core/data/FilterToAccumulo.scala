/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.data

import FilterToAccumulo._
import com.vividsolutions.jts.geom.Polygon
import geomesa.core.index.SpatioTemporalIndexSchema
import geomesa.core.index.SpatioTemporalIndexSchema._
import geomesa.utils.text.WKTUtils
import java.util.Date
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{Interval => JodaInterval, DateTime, Duration, DateTimeZone}
import org.opengis.filter._
import org.opengis.filter.expression._
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._
import org.opengis.geometry.BoundingBox
import org.opengis.temporal.Period
import scala.collection.JavaConverters._
import scala.util.Try

object FilterToAccumulo {
  val IntervalBound = 0
  val LowerBound = 1
  val UpperBound = 2

  val TypeGeom = "geom"
  val TypeTime = "time"
  val TypeOther = "other"

  // core of set operations on JodaIntervals, Polygons, and Filters
  trait SetLike[T] {
    def undefined: Option[T] = None
    def isDefined(a: Option[T]): Boolean = a.isDefined
    def isUndefined(a: Option[T]): Boolean = !isDefined(a)
    def everything: Option[T]
    def isEverything(a: Option[T]): Boolean = a == everything
    // the difference between "nothing" and "undefined" is that "nothing" denotes
    // an empty set, whereas "undefined" denotes no set of any kind has been identified
    def nothing: Option[T]
    def isNothing(a: Option[T]): Boolean = a == nothing
    def intersection(a: T, b: T): Option[T]
    def intersection(a: Option[T], b: Option[T]): Option[T] = a match {
      case u if isUndefined(u) => b
      case n if isNothing(n) => nothing
      case e if isEverything(e) => b
      case _ => b match {
        case u if isUndefined(u) => a
        case n if isNothing(n) => nothing
        case e if isEverything(e) => a
        case _ => intersection(a.get, b.get)
      }
    }
    def union(a: T, b: T): Option[T]
    def union(a: Option[T], b: Option[T]): Option[T] = a match {
      case u if isUndefined(u) => everything
      case n if isNothing(n) => b
      case e if isEverything(e) => everything
      case _ => b match {
        case u if isUndefined(u) => everything
        case n if isNothing(n) => a
        case e if isEverything(e) => everything
        case _ => union(a.get, b.get)
      }
    }
  }

  implicit object SetLikeInterval extends SetLike[JodaInterval] {
    val everything = Some(new JodaInterval(minDateTime, maxDateTime))
    val nothing = Some(new JodaInterval(minDateTime, minDateTime))
    def intersection(a: JodaInterval, b: JodaInterval): Option[JodaInterval] =
      if (a.overlaps(b)) Some(a.overlap(b))
      else nothing
    def union(a: JodaInterval, b: JodaInterval): Option[JodaInterval] =
      if (a.overlaps(b) || a.abuts(b)) Some(new JodaInterval(
        math.min(a.getStartMillis, b.getStartMillis),
        math.max(a.getEndMillis, b.getEndMillis),
        DateTimeZone.forID("UTC")
      ))
      else throw new Exception("Cannot (yet) union two disjoint JodaIntervals.")
  }

  implicit object SetLikePolygon extends SetLike[Polygon] {
    val everything = Some(everywhere)
    val tiny = 1e-8
    val nothing = Some(WKTUtils.read(
      s"POLYGON(($tiny $tiny,$tiny $tiny,$tiny $tiny,$tiny $tiny,$tiny $tiny))")
      .asInstanceOf[Polygon])
    def intersection(a: Polygon, b: Polygon): Option[Polygon] =
      if (a.intersects(b)) a.intersection(b) match {
        case p: Polygon => Some(p)
        case _ => nothing  // if the intersection isn't a polygon, we treat is as non-intersecting
      }
      else nothing
    def union(a: Polygon, b: Polygon): Option[Polygon] =
      if (a.intersects(b)) a.union(b) match {
        case p: Polygon => Some(p)
        case _ => throw new Exception("Cannot (yet) union two Polygons whose union is not a Polygon")
      }
      else throw new Exception("Cannot (yet) union two disjoint Polygons.")
  }

  implicit object SetLikeFilter extends SetLike[Filter] {
    val everything = Some(Filter.INCLUDE)
    val nothing = Some(Filter.EXCLUDE)
    def intersection(a: Filter, b: Filter): Option[Filter] = Some(
      ECQL.toFilter(
        "( " + ECQL.toCQL(a) + " ) AND ( " + ECQL.toCQL(b) + " )"
      )
    )
    def union(a: Filter, b: Filter): Option[Filter] = Some(
      ECQL.toFilter(
        "( " + ECQL.toCQL(a) + " ) OR ( " + ECQL.toCQL(b) + " )"
      )
    )
  }

  implicit object SetLikeExtraction extends SetLike[Extraction] {
    val setOpsPolygon = implicitly[SetLike[Polygon]]
    val setOpsInterval = implicitly[SetLike[JodaInterval]]
    val setOpsFilter = implicitly[SetLike[Filter]]
    val everything: Option[Extraction] = Some(Extraction(
      setOpsPolygon.everything, setOpsInterval.everything, setOpsFilter.everything
    ))
    val nothing = Some(Extraction(
      setOpsPolygon.nothing, setOpsInterval.nothing, setOpsFilter.nothing
    ))
    def intersection(a: Extraction, b: Extraction): Option[Extraction] = Some(Extraction(
      setOpsPolygon.intersection(a.polygon, b.polygon),
      setOpsInterval.intersection(a.interval, b.interval),
      setOpsFilter.intersection(a.filter, b.filter)
    ))
    def union(a: Extraction, b: Extraction): Option[Extraction] = Some(Extraction(
      setOpsPolygon.union(a.polygon, b.polygon),
      setOpsInterval.union(a.interval, b.interval),
      setOpsFilter.union(a.filter, b.filter)
    ))
  }
}


case class Extraction(polygon: Option[Polygon], interval: Option[JodaInterval], filter: Option[Filter]) {
  def getType: String = SetLikePolygon.isDefined(polygon) match {
    case true  => SetLikeInterval.isDefined(interval) match {
      case true => TypeOther
      case false => TypeGeom
    }
    case false => SetLikeInterval.isDefined(interval) match {
      case true => TypeTime
      case false => TypeOther
    }
  }
}

case class FilterExtractor(geometryPropertyName: String, temporalPropertyNames: Set[String]) {

  def getPropertyName(expression: Expression): Option[String] = expression match {
    case p: PropertyName => Some(p.getPropertyName)
    case _               => None
  }

  implicit def bounds2poly(bbox: BoundingBox): Polygon = WKTUtils.read("POLYGON((" +
    bbox.getMinX + " " + bbox.getMinY + "," +
    bbox.getMinX + " " + bbox.getMaxY + "," +
    bbox.getMaxX + " " + bbox.getMaxY + "," +
    bbox.getMaxX + " " + bbox.getMinY + "," +
    bbox.getMinX + " " + bbox.getMinY + "," +
    "))").asInstanceOf[Polygon]

  def getGeometry(expression: Expression): Option[Polygon] = expression.evaluate(null) match {
    case poly: Polygon     => Some(poly)
    case bbox: BoundingBox => Some(bounds2poly(bbox))
    case _                 => SetLikePolygon.nothing
  }

  def getInterval(expression: Expression, boundSpecifier: Int): Option[JodaInterval] = {
    val eval = expression.evaluate(null)

    eval match {
      case p: Period => Some(new JodaInterval(
        p.getBeginning.getPosition.getDate.getTime,
        p.getEnding.getPosition.getDate.getTime,
        DateTimeZone.forID("UTC")
      ))
      case d: Date => boundSpecifier match {
        case LowerBound => Some(new JodaInterval(
          d.getTime,
          SpatioTemporalIndexSchema.maxDateTime.getMillis,
          DateTimeZone.forID("UTC")
        ))
        case UpperBound => Some(new JodaInterval(
          SpatioTemporalIndexSchema.minDateTime.getMillis,
          d.getTime,
          DateTimeZone.forID("UTC")
        ))
        case _ => throw new Exception(
          s"Invalid bounds specification ($boundSpecifier) for Date")
      }
      case other => throw new Exception(
        s"Invalid interval type ${other.getClass.getCanonicalName}")
    }
  }

  def processBinaryGeometryPredicate(filter: BinarySpatialOperator): Option[Extraction] = {
    getPropertyName(filter.getExpression1) match {
      case Some(property) if property == geometryPropertyName =>
        Some(Extraction(getGeometry(filter.getExpression2), SetLikeInterval.undefined, SetLikeFilter.everything))
      case _ => Some(Extraction(SetLikePolygon.undefined, SetLikeInterval.undefined, SetLikeFilter.everything))
    }
  }

  def processBinaryTemporalPredicate(filter: BinaryTemporalOperator, bound: Int): Option[Extraction] = {
    getPropertyName(filter.getExpression1) match {
      case Some(property) if temporalPropertyNames.contains(property) =>
        Some(Extraction(SetLikePolygon.undefined, getInterval(filter.getExpression2, bound), SetLikeFilter.everything))
      case _ => Some(Extraction(SetLikePolygon.undefined, SetLikeInterval.undefined, SetLikeFilter.everything))
    }
  }

  val fmt = ISODateTimeFormat.dateTime()
  def extractDate(v: Any): Try[Date] = v match {
    case d: Date => util.Success(d)
    case s: String => Try(fmt.parseDateTime(s).toDate)
    case _ => util.Failure(new Exception("Invalid date type"))
  }

  def processBetween(filter: PropertyIsBetween): Option[Extraction] = {
    getPropertyName(filter.getExpression) match {
      case Some(property) if property == geometryPropertyName =>
        throw new Exception("BETWEEN is not supported as a geometric predicate")

      case Some(property) if temporalPropertyNames.contains(property) =>
        val result = for {
          childLeft  <- extractDate(filter.getLowerBoundary.evaluate(null))
          childRight <- extractDate(filter.getUpperBoundary.evaluate(null))
        } yield {
          Some(Extraction(
            SetLikePolygon.undefined,
            Some(new JodaInterval(
              childLeft.asInstanceOf[Date].getTime,
              childRight.asInstanceOf[Date].getTime,
              DateTimeZone.forID("UTC")
            )),
            SetLikeFilter.everything
          ))
        }
        result match {
          case util.Success(d) => d
          case util.Failure(t) => throw t
        }

      case _ => throw new Exception(
        "BETWEEN is only supported on direct time property names, not nested expressions")
    }
  }

  def processIsEqualsTo(filter: PropertyIsEqualTo): Option[Extraction] = {
    getPropertyName(filter.getExpression1) match {
      case Some(property) if property == geometryPropertyName =>
        throw new Exception("EQUALS is not supported as a geometric predicate")

      case Some(property) if temporalPropertyNames.contains(property) =>
        // Specify an interval of one minute around requested time
        val child = filter.getExpression2.evaluate(null).asInstanceOf[Date]
        val start = new DateTime(child.getTime).withZone(DateTimeZone.forID("UTC"))
          .withSecondOfMinute(0)
        val duration = Duration.standardMinutes(1).toIntervalFrom(start)
        Some(Extraction(SetLikePolygon.undefined, Some(duration), SetLikeFilter.everything))

      case _ => Some(Extraction(
        SetLikePolygon.undefined, SetLikeInterval.undefined, Option(filter))) // default pass-through
    }
  }

  def and[T : SetLike](a: Option[T], b: Option[T]): Option[T] = {
    val setOperations = implicitly[SetLike[T]]
    setOperations.intersection(a, b)
  }

  // AND can have more than two children
  def processAnd(children: Seq[Filter]): Option[Extraction] = {
    val extractions = children.map(extractAndModify)
    extractions.foldLeft(SetLikeExtraction.everything)((extSoFar, extChild) =>
      and(extSoFar, extChild))
  }

  def or[T : SetLike](a: Option[T], b: Option[T]): Option[T] = {
    val setOperations = implicitly[SetLike[T]]
    setOperations.union(a, b)
  }

  // NB:  in the long-term, this routine should probably be able to act upon
  // a list of extractions (both input and output), because the Real Answer
  // to handling disjunction is to realize that every OR represents,
  // potentially, a "Y" in the processing that results in two independent
  // sibling queries; in fact, each time an OR acts, the number of total
  // queries doubles (albeit with some possible simplification), which is
  // why we are taking the shorter, faster answer for now:  if the two
  // child-filters of OR are of the same type, then we can do some
  // simplification, but if they are of differing types, then we lose most
  // of the extraction performed so far (beneath them)
  def processOr(children: Seq[Filter]): Option[Extraction] = {
    // complain if there are more than two children (for now)
    if (children.size != 2)
      throw new Exception("OR can only handle two children (for now)")

    val extractions = children.map(extractAndModify)
    val extractionLeft: Option[Extraction] = extractions.head
    val extractionRight: Option[Extraction] = extractions.last

    // blend the two filters
    val blendedFilter: Option[Filter] = or(
      extractionLeft.map(_.filter).getOrElse(null),
      extractionRight.map(_.filter).getOrElse(null)
    )
    val completeFilter: Option[Filter] = or(
      children.headOption,
      children.lastOption
    )

    // apply special handling to detect unsupported disjunction
    val typeLeft = extractionLeft.map(_.getType).getOrElse(TypeOther)
    val typeRight = extractionRight.map(_.getType).getOrElse(TypeOther)
    if (typeLeft == typeRight) {
      // the two child-extraction-types match:  they might be blended
      val netPolygon: Option[Polygon] = typeLeft match {
        case TypeGeom => or(
          extractionLeft.map(_.polygon.getOrElse(null)),
          extractionRight.map(_.polygon).getOrElse(null))
        case _ => SetLikePolygon.undefined
      }
      val netInterval: Option[JodaInterval] = typeLeft match {
        case TypeTime => or(
          extractionLeft.map(_.interval.getOrElse(null)),
          extractionRight.map(_.interval.getOrElse(null)))
        case _ => SetLikeInterval.undefined
      }
      val netFilter = if (SetLikePolygon.isDefined(netPolygon) || SetLikeInterval.isDefined(netInterval))
        blendedFilter else completeFilter
      Some(Extraction(
        netPolygon,
        netInterval,
        netFilter
      ))
    } else {
      // the two child-extraction types do not match; they cannot be blended
      Some(Extraction(
        SetLikePolygon.undefined,
        SetLikeInterval.undefined,
        completeFilter
      ))
    }
  }

  //@TODO flesh out the list of geo-time filters supported (and "NOT")
  def extractAndModify(filter: Filter): Option[Extraction] = filter match {
    case null => Some(Extraction(
      SetLikePolygon.undefined, SetLikeInterval.undefined, SetLikeFilter.everything))
    // geometric filters
    case bbox: BBOX => processBinaryGeometryPredicate(bbox)
    case intx: Intersects => processBinaryGeometryPredicate(intx)
    case ovlp: Overlaps => processBinaryGeometryPredicate(ovlp)
    // temporal filters
    case before: Before => processBinaryTemporalPredicate(before, UpperBound)
    case after: After => processBinaryTemporalPredicate(after, LowerBound)
    case during: During => processBinaryTemporalPredicate(during, IntervalBound)
    // (kinda') shared filters
    case between: PropertyIsBetween => processBetween(between)
    case equals: PropertyIsEqualTo => processIsEqualsTo(equals)
    // logical filters
    case and: And => processAnd(and.getChildren.asScala)
    case or: Or => processOr(or.getChildren.asScala)
    // unhandled filters
    case _ => Some(Extraction(
      SetLikePolygon.undefined, SetLikeInterval.undefined, Option(filter))) // default pass-through
  }
}
