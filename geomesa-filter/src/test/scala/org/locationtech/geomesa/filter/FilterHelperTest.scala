/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import java.time.temporal.ChronoUnit
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.{IsGreaterThanImpl, IsLessThenImpl, LiteralExpressionImpl}
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.Bounds.Bound
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterHelperTest extends Specification {

  val sft = SimpleFeatureTypes.createType("FilterHelperTest",
    "dtg:Date,number:Int,a:Int,b:Int,c:Int,*geom:Point:srid=4326")

  def updateFilter(filter: Filter): Filter = QueryPlanFilterVisitor.apply(sft, filter)

  def toInterval(dt1: String, dt2: String, inclusive: Boolean = true): Bounds[ZonedDateTime] = {
    val s = Option(Converters.convert(dt1, classOf[Date])).map(d => ZonedDateTime.ofInstant(toInstant(d), ZoneOffset.UTC))
    val e = Option(Converters.convert(dt2, classOf[Date])).map(d => ZonedDateTime.ofInstant(toInstant(d), ZoneOffset.UTC))
    Bounds(Bound(s, if (s.isDefined) inclusive else false), Bound(e, if (e.isDefined) inclusive else false))
  }

  "FilterHelper" should {

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
      val filter = ff.bbox(ff.property("geom"), -181, -91, 181, 91, "4326")
      val updated = updateFilter(filter)
      updated mustEqual Filter.INCLUDE
    }

    "be idempotent with bbox" >> {
      val filter = ff.bbox(ff.property("geom"), -181, -91, 181, 91, "4326")
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
      import scala.collection.JavaConversions._
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
      flattened.asInstanceOf[Or].getChildren.map(_.toString) must
          containTheSameElementsAs((0 until count).map(i => s"[ a equals $i ]"))
    }
  }
}
