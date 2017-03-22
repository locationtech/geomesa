/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import java.util.Date

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.Converters
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterHelperTest extends Specification {

  val ff = CommonFactoryFinder.getFilterFactory2

  val MinDateTime = Converters.convert(FilterHelper.MinDateTime.toDate, classOf[String])
  val MaxDateTime = Converters.convert(FilterHelper.MaxDateTime.toDate, classOf[String])

  def updateFilter(filter: Filter) = filter.accept(new QueryPlanFilterVisitor(null), null).asInstanceOf[Filter]

  def toInterval(dt1: String, dt2: String): (DateTime, DateTime) = {
    val s = Converters.convert(dt1, classOf[Date])
    val e = Converters.convert(dt2, classOf[Date])
    (new DateTime(s, DateTimeZone.UTC), new DateTime(e, DateTimeZone.UTC))
  }

  "FilterHelper" should {

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
      val predicates = Seq("dtg DURING 2016-01-01T00:00:00.000Z/2016-01-02T00:00:00.000Z",
        "dtg BETWEEN '2016-01-01T00:00:00.000Z' AND '2016-01-02T00:00:00.000Z'")
      forall(predicates) { predicate =>
        val filter = ECQL.toFilter(predicate)
        val intervals = FilterHelper.extractIntervals(filter, "dtg")
        intervals mustEqual FilterValues(Seq(toInterval("2016-01-01T00:00:00.000Z", "2016-01-02T00:00:00.000Z")))
      }
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
        "dtg > '2016-01-01T00:00:00.000Z'",
        "dtg >= '2016-01-01T00:00:00.000Z'",
        "dtg AFTER 2016-01-01T00:00:00.000Z"
      )
      forall(filters) { cql =>
        val filter = ECQL.toFilter(cql)
        val intervals = FilterHelper.extractIntervals(filter, "dtg")
        intervals mustEqual FilterValues(Seq(toInterval("2016-01-01T00:00:00.000Z", MaxDateTime)))
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
        intervals mustEqual FilterValues(Seq(toInterval("2016-01-01T00:00:01.000Z", MaxDateTime)))
      }
    }

    "extract interval from simple before" >> {
      val filters = Seq(
        "dtg < '2016-01-01T00:00:00.000Z'",
        "dtg <= '2016-01-01T00:00:00.000Z'",
        "dtg BEFORE 2016-01-01T00:00:00.000Z"
      )
      forall(filters) { cql =>
        val filter = ECQL.toFilter(cql)
        val intervals = FilterHelper.extractIntervals(filter, "dtg")
        intervals mustEqual FilterValues(Seq(toInterval(MinDateTime, "2016-01-01T00:00:00.000Z")))
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
        intervals mustEqual FilterValues(Seq(toInterval(MinDateTime, "2016-01-01T00:00:00.000Z")))
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
