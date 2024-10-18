/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import org.geotools.api.filter._
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.{IsGreaterThanImpl, IsLessThenImpl, LiteralExpressionImpl, OrImpl}
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.Bounds.Bound
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.time.temporal.ChronoUnit
import java.time.{ZoneOffset, ZonedDateTime}
import java.util
import java.util.Date

@RunWith(classOf[JUnitRunner])
class FilterHelperTest extends Specification {

  val sft = SimpleFeatureTypes.createType("FilterHelperTest",
    "dtg:Date,number:Int,a:Int,b:Int,c:Int,*geom:Point:srid=4326,name:String")

  def updateFilter(filter: Filter): Filter = QueryPlanFilterVisitor.apply(sft, filter)

  def toInterval(dt1: String, dt2: String, inclusive: Boolean = true): Bounds[ZonedDateTime] = {
    val s = Option(Converters.convert(dt1, classOf[Date])).map(d => ZonedDateTime.ofInstant(toInstant(d), ZoneOffset.UTC))
    val e = Option(Converters.convert(dt2, classOf[Date])).map(d => ZonedDateTime.ofInstant(toInstant(d), ZoneOffset.UTC))
    Bounds(Bound(s, if (s.isDefined) inclusive else false), Bound(e, if (e.isDefined) inclusive else false))
  }

  "FilterHelper" should {

    "optimize the inArray function" >> {
      val sf = new ScalaSimpleFeature(sft, "1",
        Array(new Date(), new Integer(1), new Integer(2), new Integer(3), new Integer(4), WKTUtils.read("POINT(1 5)"), "foo"))

      val originalFilter = ff.equals(
        ff.function("inArray", ff.property("name"), ff.literal(new util.ArrayList(util.Arrays.asList("foo", "bar")))),
        ff.literal(java.lang.Boolean.TRUE)
      )
      val optimizedFilter = updateFilter(originalFilter)

      optimizedFilter.isInstanceOf[OrImpl] mustEqual(true)
      originalFilter.evaluate(sf) mustEqual(true)
      optimizedFilter.evaluate(sf) mustEqual(true)

      // Show that either order works.
      val reverseOrder = ff.equals(
        ff.literal(java.lang.Boolean.TRUE),
        ff.function("inArray",
          ff.property("name"),
          ff.literal(new util.ArrayList(util.Arrays.asList("foo", "bar"))))
      )
      val optimizedReverseOrder = updateFilter(reverseOrder)
      optimizedReverseOrder.isInstanceOf[OrImpl] mustEqual(true)
      reverseOrder.evaluate(sf) mustEqual(true)
      optimizedReverseOrder.evaluate(sf) mustEqual(true)
    }

    "evaluate functions with 0 arguments" >> {
      val filter = ECQL.toFilter("dtg < currentDate()")
      val updated = updateFilter(filter)
      updated.asInstanceOf[IsLessThenImpl].getExpression2.isInstanceOf[LiteralExpressionImpl] mustEqual true
    }

    "evaluate functions with 1 argument" >> {
      val filter = ECQL.toFilter("dtg > currentDate('P2D')")
      val updated = updateFilter(filter)
      updated.asInstanceOf[IsGreaterThanImpl].getExpression2.isInstanceOf[LiteralExpressionImpl] mustEqual true
    }

    "evaluate functions representing the last day" >> {
      val filter = ECQL.toFilter("dtg > currentDate('-P1D') AND dtg < currentDate()")
      val updated = updateFilter(filter)
      val intervals = FilterHelper.extractIntervals(updated, "dtg", handleExclusiveBounds = true)
      intervals.values(0).lower.value.get.until(
        intervals.values(0).upper.value.get, ChronoUnit.HOURS) must beCloseTo(24l, 2)
    }

    "evaluate functions with math" >> {
      val filter = ECQL.toFilter("number < 1+2")
      val updated = updateFilter(filter)
      updated.asInstanceOf[IsLessThenImpl].getExpression2.evaluate(null) mustEqual 3
    }

    "fix out of bounds bbox" >> {
      val filter = ECQL.toFilter("bbox(geom,-181,-91,181,91)")
      val updated = updateFilter(filter)
      updated mustEqual Filter.INCLUDE
    }

    "be idempotent with bbox" >> {
      val filter = ECQL.toFilter("bbox(geom,-181,-91,181,91)")
      val updated = updateFilter(filter)
      val reupdated = updateFilter(updated)
      ECQL.toCQL(updated) mustEqual ECQL.toCQL(reupdated)
    }

    "be idempotent with dwithin" >> {
      val filter = ff.dwithin(ff.property("geom"), ff.literal("LINESTRING (-45 0, -90 45)"), 1000, "meters")
      val updated = updateFilter(filter)
      val reupdated = updateFilter(updated)
      ECQL.toCQL(updated) mustEqual ECQL.toCQL(reupdated)
    }

    "not modify valid intersects" >> {
      val filter = ff.intersects(ff.property("geom"), ff.literal("POLYGON((45 23, 45 27, 48 27, 48 23, 45 23))"))
      val updated = updateFilter(filter)
      ECQL.toCQL(updated) mustEqual "INTERSECTS(geom, POLYGON ((45 23, 45 27, 48 27, 48 23, 45 23)))"
    }

    "fix IDL polygons in intersects" >> {
      val filter = ff.intersects(ff.property("geom"), ff.literal("POLYGON((-150 23,-164 11,45 23,49 30,-150 23))"))
      val updated = updateFilter(filter)
      ECQL.toCQL(updated) mustEqual "INTERSECTS(geom, POLYGON ((-180 12.271523178807946, -180 24.304347826086957, " +
          "-150 23, -164 11, -180 12.271523178807946))) OR INTERSECTS(geom, POLYGON ((180 24.304347826086957, " +
          "180 12.271523178807946, 45 23, 49 30, 180 24.304347826086957)))"
    }

    "be idempotent with intersects" >> {
      val filter = ff.intersects(ff.property("geom"), ff.literal("POLYGON((-150 23,-164 11,45 23,49 30,-150 23))"))
      val updated = updateFilter(filter)
      val reupdated = updateFilter(updated)
      ECQL.toCQL(updated) mustEqual ECQL.toCQL(reupdated)
    }

    "extract interval from simple during and between" >> {
      val predicates = Seq(("dtg DURING 2016-01-01T00:00:00.000Z/2016-01-02T00:00:00.000Z", false),
        ("dtg BETWEEN '2016-01-01T00:00:00.000Z' AND '2016-01-02T00:00:00.000Z'", true))
      forall(predicates) { case (predicate, inclusive) =>
        val filter = ECQL.toFilter(predicate)
        val intervals = FilterHelper.extractIntervals(filter, "dtg")
        intervals mustEqual FilterValues(Seq(toInterval("2016-01-01T00:00:00.000Z", "2016-01-02T00:00:00.000Z", inclusive)))
      }
    }

    "extract interval from narrow during" >> {
      val filter = ECQL.toFilter("dtg DURING 2016-01-01T00:00:00.000Z/T1S")
      val intervals = FilterHelper.extractIntervals(filter, "dtg", handleExclusiveBounds = true)
      intervals mustEqual FilterValues(Seq(toInterval("2016-01-01T00:00:00.000Z", "2016-01-01T00:00:01.000Z", inclusive = false)))
    }

    "extract interval with exclusive endpoints from simple during and between" >> {
      val during = "dtg DURING 2016-01-01T00:00:00.000Z/2016-01-02T00:00:00.000Z"
      val between = "dtg BETWEEN '2016-01-01T00:00:00.000Z' AND '2016-01-02T00:00:00.000Z'"

      val dIntervals = FilterHelper.extractIntervals(ECQL.toFilter(during), "dtg", handleExclusiveBounds = true)
      val bIntervals = FilterHelper.extractIntervals(ECQL.toFilter(between), "dtg", handleExclusiveBounds = true)

      dIntervals mustEqual FilterValues(Seq(toInterval("2016-01-01T00:00:01.000Z", "2016-01-01T23:59:59.000Z")))
      bIntervals mustEqual FilterValues(Seq(toInterval("2016-01-01T00:00:00.000Z", "2016-01-02T00:00:00.000Z")))
    }

    "extract interval from simple equals" >> {
      val filters = Seq("dtg = '2016-01-01T00:00:00.000Z'", "dtg TEQUALS 2016-01-01T00:00:00.000Z")
      forall(filters) { cql =>
        val filter = ECQL.toFilter(cql)
        val intervals = FilterHelper.extractIntervals(filter, "dtg")
        intervals mustEqual FilterValues(Seq(toInterval("2016-01-01T00:00:00.000Z", "2016-01-01T00:00:00.000Z")))
      }
    }

    "extract interval from simple after" >> {
      val filters = Seq(
        ("dtg > '2016-01-01T00:00:00.000Z'", false),
        ("dtg >= '2016-01-01T00:00:00.000Z'", true),
        ("dtg AFTER 2016-01-01T00:00:00.000Z", false)
      )
      forall(filters) { case (cql, inclusive) =>
        val filter = ECQL.toFilter(cql)
        val intervals = FilterHelper.extractIntervals(filter, "dtg")
        intervals mustEqual FilterValues(Seq(toInterval("2016-01-01T00:00:00.000Z", null, inclusive)))
      }
    }

    "extract interval from simple after with exclusive bounds" >> {
      val filters = Seq(
        "dtg > '2016-01-01T00:00:00.000Z'",
        "dtg >= '2016-01-01T00:00:01.000Z'",
        "dtg AFTER 2016-01-01T00:00:00.000Z"
      )
      forall(filters) { cql =>
        val filter = ECQL.toFilter(cql)
        val intervals = FilterHelper.extractIntervals(filter, "dtg", handleExclusiveBounds = true)
        intervals mustEqual FilterValues(Seq(toInterval("2016-01-01T00:00:01.000Z", null)))
      }
    }

    "extract interval from simple before" >> {
      val filters = Seq(
        ("dtg < '2016-01-01T00:00:00.000Z'", false),
        ("dtg <= '2016-01-01T00:00:00.000Z'", true),
        ("dtg BEFORE 2016-01-01T00:00:00.000Z", false)
      )
      forall(filters) { case (cql, inclusive) =>
        val filter = ECQL.toFilter(cql)
        val intervals = FilterHelper.extractIntervals(filter, "dtg")
        intervals mustEqual FilterValues(Seq(toInterval(null, "2016-01-01T00:00:00.000Z", inclusive)))
      }
    }

    "extract interval from simple before with exclusive bounds" >> {
      val filters = Seq(
        "dtg < '2016-01-01T00:00:00.001Z'",
        "dtg <= '2016-01-01T00:00:00.000Z'",
        "dtg BEFORE 2016-01-01T00:00:00.001Z"
      )
      forall(filters) { cql =>
        val filter = ECQL.toFilter(cql)
        val intervals = FilterHelper.extractIntervals(filter, "dtg", handleExclusiveBounds = true)
        intervals mustEqual FilterValues(Seq(toInterval(null, "2016-01-01T00:00:00.000Z")))
      }
    }

    "extract bounds from case-insensitive matches" >> {
      def matcher(f: String, expected: Seq[String], rel: Option[String] => Option[String]): MatchResult[Any] = {
        val res = FilterHelper.extractAttributeBounds(ECQL.toFilter(f), "name", classOf[String])
        forall(res.values)(b => rel(b.lower.value) mustEqual b.upper.value)
        res.values.map(_.lower.value.orNull) must containTheSameElementsAs(expected)
      }
      def literal(f: String, expected: Seq[String]): MatchResult[Any] = matcher(f, expected, o => o)
      def wildcard(f: String, expected: Seq[String]): MatchResult[Any] =
        matcher(f, expected, o => o.map(_ + WildcardSuffix))

      literal("name ilike 'foo'", Seq("foo", "Foo", "fOo", "foO", "fOO", "FoO", "FOo", "FOO"))
      wildcard("name ilike 'foo%'", Seq("foo", "Foo", "fOo", "foO", "fOO", "FoO", "FOo", "FOO"))
      literal("name ilike 'f2o'", Seq("f2o", "F2o", "f2O", "F2O"))
      wildcard("name ilike 'f2o%'", Seq("f2o", "F2o", "f2O", "F2O"))
      // test max expansion limit of 10 chars
      literal("name ilike 'fooooooooooo'", Seq.empty)
      wildcard("name ilike 'fooooooooooo%'", Seq.empty)
      // test setting sys prop for max expansion limit
      FilterProperties.CaseInsensitiveLimit.threadLocalValue.set("2")
      try {
        literal("name ilike 'foo'", Seq.empty)
        wildcard("name ilike 'foo%'", Seq.empty)
      } finally {
        FilterProperties.CaseInsensitiveLimit.threadLocalValue.remove()
      }
    }

    "ignore OR clauses with differing attributes when extracting bounds" >> {
      val cql1 = "((BBOX(geom, 0.0,0.0,10.0,10.0) OR " +
        "BBOX(geom, 20.0,20.0,30.0,30.0)) AND " +
        "(dtg BETWEEN 2021-05-05T00:00:00+00:00 AND 2021-05-10T00:00:00+00:00 OR " +
        "dtg BETWEEN 2021-05-01T00:00:00+00:00 AND 2021-05-04T00:00:00+00:00)) AND " +
        "((BBOX(geom, 0.0,0.0,10.0,10.0) OR " +
        "dtg BETWEEN 2021-05-01T00:00:00+00:00 AND 2021-05-04T00:00:00+00:00) AND " +
        "(dtg BETWEEN 2021-05-05T00:00:00+00:00 AND 2021-05-10T00:00:00+00:00 OR " +
        "BBOX(geom, 20.0,20.0,30.0,30.0)))"

      val cql2 = "dtg BETWEEN 2021-05-05T00:00:00+00:00 AND 2021-05-10T00:00:00+00:00 OR " +
        "dtg BETWEEN 2021-05-01T00:00:00+00:00 AND 2021-05-04T00:00:00+00:00"

      val intervals1 = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql1), "dtg", classOf[Date])
      val intervals2 = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql2), "dtg", classOf[Date])
      intervals1 mustEqual intervals2
    }

    "ignore OR clauses with differing attributes when extracting bounds" >> {
      val cql1 = "((BBOX(geom, 0.0,0.0,10.0,10.0) OR " +
        "BBOX(geom, 20.0,20.0,30.0,30.0)) AND " +
        "(dtg BETWEEN 2021-05-05T00:00:00+00:00 AND 2021-05-10T00:00:00+00:00 OR " +
        "dtg BETWEEN 2021-05-01T00:00:00+00:00 AND 2021-05-04T00:00:00+00:00)) AND " +
        "((BBOX(geom, 0.0,0.0,10.0,10.0) OR " +
        "dtg BETWEEN 2021-05-01T00:00:00+00:00 AND 2021-05-04T00:00:00+00:00) AND " +
        "(dtg BETWEEN 2021-05-05T00:00:00+00:00 AND 2021-05-10T00:00:00+00:00 OR " +
        "BBOX(geom, 20.0,20.0,30.0,30.0)))"

      val cql2 = "dtg BETWEEN 2021-05-05T00:00:00+00:00 AND 2021-05-10T00:00:00+00:00 OR " +
        "dtg BETWEEN 2021-05-01T00:00:00+00:00 AND 2021-05-04T00:00:00+00:00"

      val intervals1 = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql1), "dtg", classOf[Date])
      val intervals2 = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql2), "dtg", classOf[Date])
      intervals1 mustEqual intervals2
    }

    "ignore OR clauses with differing attributes (2)" >> {
      val cql1 = "(BBOX(geom, 20.0,20.0,30.0,30.0) OR " +
        "dtg DURING 2021-05-05T00:00:00.000Z/2021-05-10T00:00:00.000Z) AND " +
        "dtg DURING 2021-05-01T00:00:00.000Z/2021-05-04T00:00:00.000Z"

      val cql2 = "dtg DURING 2021-05-01T00:00:00.000Z/2021-05-04T00:00:00.000Z"

      val intervals1 = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql1), "dtg", classOf[Date])
      val intervals2 = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql2), "dtg", classOf[Date])
      intervals1 mustEqual intervals2
    }

    "ignore OR clauses with differing attributes without losing valid clauses" >> {
      val cql1 = "(BBOX(geom, 20.0,20.0,30.0,30.0) AND " +
        "dtg DURING 2021-05-05T00:00:00.000Z/2021-05-10T00:00:00.000Z) OR " +
        "dtg DURING 2021-05-01T00:00:00.000Z/2021-05-04T00:00:00.000Z"

      val cql2 = "dtg DURING 2021-05-05T00:00:00.000Z/2021-05-10T00:00:00.000Z OR " +
        "dtg DURING 2021-05-01T00:00:00.000Z/2021-05-04T00:00:00.000Z"

      val intervals1 = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql1), "dtg", classOf[Date])
      val intervals2 = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql2), "dtg", classOf[Date])
      intervals1 mustEqual intervals2
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 8832d83354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91a82cfd3f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 16bdf7af39 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9c471ea3eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fc737139c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4010468d90 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 91a82cfd3f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fc737139c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91a82cfd3f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 3c6964ab43 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16bdf7af39 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9c471ea3eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> fc737139c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4010468d90 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91a82cfd3f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> locationtech-main
=======
>>>>>>> 1b0b697d7d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8832d83354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b0b697d7d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
    "remove interleaving bounds for AND filter" >> {
      var cql = "S < 3 AND S < 2 AND S < 4"
      var bounds = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql), "S", classOf[Integer])
      bounds.values must haveSize(1)
      bounds.values.exists(_ == Bounds(Bound(None, false), Bound(Some(2), false))) must beTrue
      cql = "S >= 3 AND S >= 2 AND S >= 1"
      bounds = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql), "S", classOf[Integer])
      bounds.values must haveSize(1)
      bounds.values.exists(_ == Bounds(Bound(Some(3), true), Bound(None, false))) must beTrue
    }

    "remove interleaving bounds for OR filter" >> {
      var cql = "S < 3 OR S < 2 OR S < 1"
      var bounds = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql), "S", classOf[Integer])
      bounds.values must haveSize(1)
      bounds.values.exists(_ == Bounds(Bound(None, false), Bound(Some(3), false))) must beTrue
      cql = "S >= 3 OR S >= 2 OR S >= 1"
      bounds = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql), "S", classOf[Integer])
      bounds.values must haveSize(1)
      bounds.values.exists(_ == Bounds(Bound(Some(1), true), Bound(None, false))) must beTrue
    }

    "concatenate bounds for OR filter" >> {
      var cql = "(S > 2 AND S <= 5) OR (S > 1 AND S < 3)"
      var bounds = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql), "S", classOf[Integer])
      bounds.values must haveSize(1)
      bounds.values.exists(_ == Bounds(Bound(Some(1), false), Bound(Some(5), true))) must beTrue
      cql = "(S >= 3 AND S <= 5) OR (S > 1 AND S < 3)"
      bounds = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql), "S", classOf[Integer])
      bounds.values must haveSize(1)
      bounds.values.exists(_ == Bounds(Bound(Some(1), false), Bound(Some(5), true))) must beTrue
    }

    "don't concatenate open bounds for OR filter" >> {
      val cql = "(S > 3 AND S <= 5) OR (S > 1 AND S < 3)"
      val bounds = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql), "S", classOf[Integer])
      bounds.values must haveSize(2)
      bounds.values.exists(_ == Bounds(Bound(Some(1), false), Bound(Some(3), false))) must beTrue
      bounds.values.exists(_ == Bounds(Bound(Some(3), false), Bound(Some(5), true))) must beTrue
    }

    "remove duplicated bounds" >> {
      val cql1 = "(S > 3 AND S < 5) OR (S > 8)"
      val cql2 = "(S > 3 OR S > 8) AND (S < 5 OR S > 8)"
      val bounds1 = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql1), "S", classOf[Integer])
      val bounds2 = FilterHelper.extractAttributeBounds(ECQL.toFilter(cql2), "S", classOf[Integer])
      bounds1 mustEqual bounds2
      bounds1.values must haveSize(2)
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8832d83354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91a82cfd3f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16bdf7af39 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9c471ea3eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fc737139c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4010468d90 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b51c563993 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 33dcb1f603 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8b0bfd55f9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 307fc2b238 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> b51c563993 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7d426cb19a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f1f448d9e0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7d426cb19a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1f448d9e0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> fc737139c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fc737139c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91a82cfd3f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
=======
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 77f1dd8356 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b51c563993 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9c471ea3eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 03a1d55f8d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c6964ab43 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> aa5808a811 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 33dcb1f603 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8b0bfd55f9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 307fc2b238 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 16bdf7af39 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9c471ea3eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d19a46c929 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> fc737139c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9cc18dca86 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4010468d90 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 65018efac6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 77f1dd8356 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 19108609dc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 7d426cb19a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5f8777dc16 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f1f448d9e0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> b51333ce3c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e5abfb88a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 5d19c5d68e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> aaf3337eb7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e30217c0a7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 32aaf8f9ff (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> e9c9dbb189 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 7bbcc3e5cb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> df76300d2c (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b6d296bc29 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 735bf09be9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91a82cfd3f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b0b697d7d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8832d83354 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b0b697d7d (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
    "deduplicate OR filters" >> {
      val filters = Seq(
        ("(a > 1 AND b < 2 AND c = 3) OR (c = 3 AND a > 2 AND b < 2) OR (b < 2 AND a > 3 AND c = 3)",
            "(a > 3 OR a > 1 OR a > 2) AND c = 3 AND b < 2"),
        ("c = 3 AND ((a > 2 AND b < 2) OR (b < 2 AND a > 3))", "c = 3 AND (a > 2 OR a > 3) AND b < 2"),
        ("(a > 1) OR (c = 3)", "a > 1 OR c = 3"),
        ("(a > 1) AND (c = 3)", "a > 1 AND c = 3"),
        ("a > 1", "a > 1")
      )

      forall(filters) { case (original, expected) =>
        ECQL.toCQL(FilterHelper.simplify(ECQL.toFilter(original))) mustEqual expected
      }
    }

    "deduplicate massive OR filters without stack overflow" >> {
      import scala.collection.JavaConverters._
      // actual count to get the old code to stack overflow varies depending on environment
      // with the fix, tested up to 100k without issue, but the specs checks take a long time with that many
      val count = 1000
      val a = ff.property("a")
      var filter: Filter = ff.equal(a, ff.literal(0))
      (1 until count).foreach { i =>
        filter = ff.or(filter, ff.equal(a, ff.literal(i)))
      }
      val flattened = FilterHelper.simplify(filter)
      flattened must beAnInstanceOf[Or]
      flattened.asInstanceOf[Or].getChildren must haveLength(count)
      flattened.asInstanceOf[Or].getChildren.asScala.map(_.toString) must
          containTheSameElementsAs((0 until count).map(i => s"[ a equals $i ]"))
    }

    "evaluate functions with large IN predicate" >> {
      val string = (0 to 4000).mkString("IN(", ",", ")")
      val filter = ECQL.toFilter(string)
      updateFilter(filter) must not(throwAn[Exception])
    }

    "evaluate functions with large number of ORs" >> {
      val count = 4000
      val a = ff.property("a")
      var filter: Filter = ff.equal(a, ff.literal(0))
      (1 until count).foreach { i =>
        filter = ff.or(filter, ff.equal(a, ff.literal(i)))
      }
      updateFilter(filter) must not(throwAn[Exception])
    }

    "evaluate functions with large number of ANDs" >> {
      val count = 4000
      val a = ff.property("a")
      var filter: Filter = ff.equal(a, ff.literal(0))
      (1 until count).foreach { i =>
        filter = ff.and(filter, ff.equal(a, ff.literal(i)))
      }
      updateFilter(filter) must not(throwAn[Exception])
    }
  }
}
