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
import com.vividsolutions.jts.geom.{Polygon, Coordinate}
import geomesa.core.data.FilterToAccumulo._
import geomesa.core.index.Constants
import geomesa.utils.geometry.Geometry._
import geomesa.utils.geotools.Conversions._
import geomesa.utils.text.WKTUtils
import geomesa.utils.time.Time._
import org.geotools.data.DataUtilities
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.temporal.`object`.{DefaultPosition, DefaultInstant}
import org.joda.time.{DateTimeZone, DateTime, Interval}
import org.junit.runner.RunWith
import org.opengis.filter.{And, Or, Not, Filter}
import org.opengis.filter.spatial.DWithin
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.opengis.filter.temporal.{Before, After, During}
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormatter}
import org.opengis.temporal.Period
import org.opengis.filter.expression.{Expression, Literal}
import org.opengis.feature.`type`.AttributeDescriptor
import org.geotools.filter.LiteralExpression

@RunWith(classOf[JUnitRunner])
class FilterToAccumuloTest extends Specification {

  val WGS84       = DefaultGeographicCRS.WGS84
  val ff          = CommonFactoryFinder.getFilterFactory2
  val geomFactory = JTSFactoryFinder.getGeometryFactory
  val sft         = DataUtilities.createType("test", "id:Integer,prop:String,dtg:Date,otherGeom:Geometry:srid=4326,*geom:Point:srid=4326")
  sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")

  "BBOX queries" should {
    "set the spatial predicate and simplify the query" in {
      val q = ff.bbox("geom", -80.0, 30, -70, 40, CRS.toSRS(WGS84))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(q)
      result mustEqual Filter.INCLUDE
    }

    "set the spatial predicate to something that spans across the IDL" in {
      val q = ff.bbox("geom", -190.0, -10, -170, 10, CRS.toSRS(WGS84))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(q)
      result.toString mustEqual "[[ geom bbox POLYGON ((-180 -10, -180 10, -170 10, -170 -10, -180 -10)) ]" +
        " OR [ geom bbox POLYGON ((170 -10, 170 10, 180 10, 180 -10, 170 -10)) ]]"
    }

    "set the spatial predicate and remove from the subsequent query" in {
      val q =
        ff.and(
          ff.like(ff.property("prop"), "foo"),
          ff.bbox("geom", -80.0, 30, -70, 40, CRS.toSRS(WGS84))
        )
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(q)
      result mustEqual ff.like(ff.property("prop"), "foo")
    }
  }

  "DWithin queries" should {
    val targetPoint = geomFactory.createPoint(new Coordinate(-70, 30))

    "take in meters" in {
      val q =
        ff.dwithin(ff.property("geom"), ff.literal(targetPoint), 100.0, "meters")

      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(q)
      val expected = WKTUtils.read("POLYGON ((-70.00103642615518 29.999097895754463, -69.9989635738448 29.999097895754463, -69.9989635738448 30.000902095962825, -70.00103642615518 30.000902095962825, -70.00103642615518 29.999097895754463))").asInstanceOf[Polygon]
      val resultEnv = f2a.spatialPredicate
      resultEnv.equalsNorm(expected) must beTrue
      result.asInstanceOf[DWithin].getDistance mustEqual 0.0010364167811696486
    }
  }

  "Within queries" should {
    "set the spatial predicate and simplify the query if rectangular" in {
      val rectWithin =
        ff.within(
          ff.property("geom"),
          ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(rectWithin)
      result mustEqual Filter.INCLUDE
    }

    "set the spatial predicate and keep the geom query if not rectangular" in {
      val rectWithin =
        ff.within(
          ff.property("geom"),
          ff.literal(WKTUtils.read("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))")))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(rectWithin)
      result mustNotEqual Filter.INCLUDE
    }

    "make a rectangular within filter that spans across the IDL" in {
      val rectWithin =
        ff.within(
          ff.property("geom"),
          ff.literal(WKTUtils.read("POLYGON((-190 -10,-190 10,-170 10,-170 -10,-190 -10))")))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(rectWithin)
      result.toString mustEqual "[[ geom within POLYGON ((-180 -10, -180 10, -170 10, -170 -10, -180 -10)) ]" +
        " OR [ geom within POLYGON ((180 10, 180 -10, 170 -10, 170 10, 180 10)) ]]"
    }

    "make a non-square polygon within filter that spans across the IDL" in {
      val rectWithin =
        ff.within(
          ff.property("geom"),
          ff.literal(WKTUtils.read("POLYGON((-190 -10,-190 10,-170 10,-170 -10,-180 -30,-190 -10))")))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(rectWithin)
      result.toString mustEqual "[[ geom within POLYGON ((-180 -30, -180 10, -170 10, -170 -10, -180 -30)) ]" +
        " OR [ geom within POLYGON ((180 10, 180 -30, 170 -10, 170 10, 180 10)) ]]"
    }
  }

  "Temporal queries" should {
    "set the temporal predicate and simplify the query" in {
      val pred = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-02-01T00:00:00Z")
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      f2a.temporalPredicate mustEqual interval
      result mustEqual Filter.INCLUDE
    }

    "with spatial queries should simplify the query" in {
      val temporal = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-02-01T00:00:00Z")
      val spatial = ff.within(ff.property("geom"), ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val pred = ff.and(temporal, spatial)
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      f2a.temporalPredicate mustEqual interval
      result mustEqual Filter.INCLUDE
    }

    "with spatial queries and property queries should simplify the query" in {
      val temporal = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-02-01T00:00:00Z")
      val spatial = ff.within(ff.property("geom"), ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val prop = ff.like(ff.property("prop"), "FOO%")
      val pred = ff.and(List(temporal, spatial, prop))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      f2a.temporalPredicate mustEqual interval
      result.toString mustEqual prop.toString
    }

    "should be able to use BETWEEN with string literals" in {
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      val temporal = ff.between(ff.property("dtg"),
        ff.literal("2011-01-01T00:00:00.000Z"),
        ff.literal("2011-02-01T00:00:00.000Z"))
      val spatial = ff.within(ff.property("geom"),
        ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val pred = ff.and(List(temporal, spatial))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      f2a.temporalPredicate mustEqual interval
      result mustEqual Filter.INCLUDE
    }

    "should be able to use BETWEEN with java.util.Date" in {
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      val temporal = ff.between(ff.property("dtg"),
        ff.literal(new DateTime("2011-01-01T00:00:00.000Z").toDate),
        ff.literal(new DateTime("2011-02-01T00:00:00.000Z").toDate))
      val spatial = ff.within(ff.property("geom"),
        ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val pred = ff.and(List(temporal, spatial))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      f2a.temporalPredicate mustEqual interval
      result mustEqual Filter.INCLUDE
    }

    "should be able to use BETWEEN with org.opengis.temporal.Instant" in {
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      val s = new DefaultInstant(new DefaultPosition(new DateTime("2011-01-01T00:00:00.000Z").toDate))
      val e = new DefaultInstant(new DefaultPosition(new DateTime("2011-02-01T00:00:00.000Z").toDate))
      val temporal = ff.between(ff.property("dtg"), ff.literal(s), ff.literal(e))
      val spatial = ff.within(ff.property("geom"),
        ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val pred = ff.and(List(temporal, spatial))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      f2a.temporalPredicate mustEqual interval
      result mustEqual Filter.INCLUDE
    }
  }

  implicit class PrettyFilter(filter: Filter) {
    val dtf = ISODateTimeFormat.dateTime

    def prettyInterval(exp: Expression): String = {
      val literal = exp.asInstanceOf[Literal].evaluate(null) match {
        case lit: Literal => lit.getValue
        case p: org.opengis.temporal.Period => p
        case i: Interval => i
        case _ => throw new Exception("Unmatched literal type")
      }

      val interval = literal match {
        case null => null
        case p: org.opengis.temporal.Period => new Interval(p.getBeginning, p.getEnding)
        case i: Interval => i
        case _ => throw new Exception("Could not match interval type")
      }
      dtf.print(interval.getStartMillis) + "/" + dtf.print(interval.getEndMillis)
    }

    def toFullerString: String = filter match {
      case f: And =>
        val terms = f.getChildren.map(_.toFullerString).mkString(" AND ")
        "[ " + terms + " ]"
      case f: Or =>
        val terms = f.getChildren.map(_.toFullerString).mkString(" OR ")
        "[ " + terms + " ]"
      case f: During =>
        "[ " + f.getExpression1.toString + " DURING " + prettyInterval(f.getExpression2) + " ]"
      case f: After =>
        "[ " + f.getExpression1.toString + " AFTER " + dtf.print(f.getExpression2) + " ]"
      case f: Before =>
        "[ " + f.getExpression1.toString + " BEFORE " + dtf.print(f.getExpression2) + " ]"
      case f: Not => "[ NOT " + f.getFilter.toFullerString
      case f         => f.toString
    }
  }

  "Negations" should {
    val wholeWorld = WKTUtils.read("POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))")
    val temporal_a = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00.000Z/2011-01-20T00:00:00.000Z")
    val spatial_a = ff.bbox("geom", -80.0, 30, -70, 38, CRS.toSRS(WGS84))
    val polygon_a = WKTUtils.read("POLYGON((-80 30,-70 30,-70 38,-80 38,-80 30))")
    val polygon_not_a = wholeWorld.difference(polygon_a)
    val attribute_a = ECQL.toFilter("prop = 'foo'")

    "go round-trip without mangling literals" in {
      val not_temporal = ECQL.toFilter("not (dtg DURING 2011-01-01T00:00:00Z/2011-01-20T00:00:00Z)")
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(not_temporal)
      result.toFullerString mustEqual not_temporal.toFullerString
    }

    "handle single geometry" in {
      val filter = ff.not(spatial_a)
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(filter)

      f2a.spatialPredicate mustEqual polygon_not_a
      f2a.temporalPredicate must beNull
      result.toString mustEqual Filter.INCLUDE.toString
    }

    "handle single interval" in {
      val filter = ff.not(temporal_a)
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(filter)

      f2a.spatialPredicate must beNull
      f2a.temporalPredicate must beNull
      result.toFullerString mustEqual filter.toFullerString
    }

    "handle single non-geometry, non-interval" in {
      val filter = ff.not(attribute_a)
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(filter)

      f2a.spatialPredicate must beNull
      f2a.temporalPredicate must beNull
      result.toFullerString mustEqual filter.toFullerString
    }

    "handle conjunctive geometry + interval + attribute" in {
      val filter = ff.not(ff.and(List(spatial_a, temporal_a, attribute_a)))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(filter)

      val filter2 = ff.or(List(ff.intersects(ff.property("geom"), ff.literal(polygon_not_a)), ff.not(temporal_a), ff.not(attribute_a)))

      f2a.spatialPredicate must beNull
      f2a.temporalPredicate must beNull
      result.toFullerString mustEqual filter2.toFullerString
    }

    "handle disjunctive geometry + interval + attribute" in {
      val filter = ff.not(ff.or(List(spatial_a, temporal_a, attribute_a)))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(filter)

      val filter2 = ff.and(List(ff.not(temporal_a), ff.not(attribute_a)))

      f2a.spatialPredicate mustEqual polygon_not_a
      f2a.temporalPredicate must beNull
      result.toFullerString mustEqual filter2.toFullerString
    }
  }

  "Logic queries" should {
    "keep property queries" in {
      val temporal = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-02-01T00:00:00Z")
      val spatial = ff.within(ff.property("geom"), ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val prop1 = ff.like(ff.property("prop"), "FOO%")
      val prop2 = ff.like(ff.property("prop"), "BAR%")
      val prop = ff.and(prop1, prop2)
      val pred = ff.and(List(temporal, spatial, prop))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      f2a.temporalPredicate mustEqual interval
      result.toString mustEqual prop.toString
    }

    "handle geometric conjunctions" in {
      val spatial_a = ff.bbox("geom", -80.0, 30, -70, 38, CRS.toSRS(WGS84))
      val spatial_b = ff.bbox("geom", -80.0, 32, -70, 40, CRS.toSRS(WGS84))
      val pred = ff.and(List(spatial_a, spatial_b))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      val polygon = WKTUtils.read("POLYGON ((-80 32, -80 38, -70 38, -70 32, -80 32))")
      f2a.spatialPredicate mustEqual polygon
      result mustEqual Filter.INCLUDE
    }

    "handle geometric disjunctions" in {
      val spatial_a = ff.bbox("geom", -80.0, 30, -70, 38, CRS.toSRS(WGS84))
      val spatial_b = ff.bbox("geom", -80.0, 32, -70, 40, CRS.toSRS(WGS84))
      val pred = ff.or(List(spatial_a, spatial_b))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      val polygon = WKTUtils.read("POLYGON ((-80 30, -80 32, -80 38, -80 40, -70 40, -70 38, -70 32, -70 30, -80 30))")
      f2a.spatialPredicate mustEqual polygon
      result mustEqual Filter.INCLUDE
    }

    "handle temporal conjunctions" in {
      val temporal_a = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-01-20T00:00:00Z")
      val temporal_b = ECQL.toFilter("dtg DURING 2011-01-10T00:00:00Z/2011-02-01T00:00:00Z")
      val pred = ff.and(List(temporal_a, temporal_b))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      val interval = new Interval(
        new DateTime("2011-01-10T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-01-20T00:00:00Z", DateTimeZone.UTC))
      f2a.temporalPredicate mustEqual interval
      result mustEqual Filter.INCLUDE
    }

    "handle temporal disjunctions" in {
      val temporal_a = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-01-20T00:00:00Z")
      val temporal_b = ECQL.toFilter("dtg DURING 2011-01-10T00:00:00Z/2011-02-01T00:00:00Z")
      val pred = ff.or(List(temporal_a, temporal_b))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      f2a.temporalPredicate mustEqual interval
      result mustEqual Filter.INCLUDE
    }

    "handle degenerate disjunctions" in {
      val temporal = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-01-20T00:00:00Z")
      val spatial = ff.bbox("geom", -80.0, 30, -70, 40, CRS.toSRS(WGS84))
      val pred = ff.or(List(temporal, spatial))
      val f2a = new FilterToAccumulo(sft)
      val result = f2a.visit(pred)
      f2a.temporalPredicate mustEqual noInterval
      f2a.spatialPredicate mustEqual noPolygon
      result.toString mustEqual pred.toString
    }

    "combine nested logical functions (and reduce the filter) correctly" in {
      val temporal_a = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-01-20T00:00:00Z")
      val temporal_b = ECQL.toFilter("dtg DURING 2011-01-10T00:00:00Z/2011-02-01T00:00:00Z")
      val temporal_c = ECQL.toFilter("dtg DURING 2011-08-10T00:00:00Z/2011-09-01T00:00:00Z")
      val spatial_a = ff.bbox("geom", -80.0, 30, -70, 38, CRS.toSRS(WGS84))
      val spatial_b = ff.bbox("geom", -80.0, 32, -70, 40, CRS.toSRS(WGS84))
      val spatial_c = ff.bbox("geom", -100.0, 30, -90, 40, CRS.toSRS(WGS84))

      val predicateAndSpatial = ff.and(spatial_a, spatial_b)
      val predicateAndTemporal = ff.and(temporal_a, temporal_b)
      val predicateOrST = ff.or(temporal_c, spatial_c)
      val predicateOr = ff.or(List(predicateAndSpatial, predicateAndTemporal, predicateOrST))

      val f2a = new FilterToAccumulo(sft)
      f2a.visit(predicateOr)

      f2a.temporalPredicate mustEqual noInterval
      f2a.spatialPredicate mustEqual noPolygon
    }
  }
}
