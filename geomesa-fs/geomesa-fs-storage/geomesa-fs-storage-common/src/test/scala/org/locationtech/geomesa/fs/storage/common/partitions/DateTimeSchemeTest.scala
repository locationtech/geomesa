/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.DateParsing
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date

@RunWith(classOf[JUnitRunner])
class DateTimeSchemeTest extends Specification {

  import org.locationtech.geomesa.filter.decomposeAnd

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-02-03T10:15:30Z", "POINT (10 10)")

  "DateTimeScheme" should {

    "partition based on date" >> {
      val ps = DateTimeScheme("dtg", 2, ChronoUnit.DAYS)
      ps.getPartition(sf) mustEqual "2017-02-03"
    }

    "partition based on date with slash delimiter" >> {
      val ps = DateTimeScheme("dtg", 2, ChronoUnit.HOURS)
      ps.getPartition(sf) mustEqual "2017/034/10"
    }

    "partition based on week" >> {
      val ps = PartitionSchemeFactory.load(sft, "weekly")
      ps must beAnInstanceOf[DateTimeScheme]
      ps.getPartition(sf) mustEqual "2017/W05"
      val tenWeeksOut = ScalaSimpleFeature.create(sft, "1", "test", 10,
        Date.from(Instant.parse("2017-01-01T00:00:00Z").plus(9*7 + 1, ChronoUnit.DAYS)), "POINT (10 10)")
      ps.getPartition(tenWeeksOut) mustEqual "2017/W10"
    }

    "simplify filters" >> {
      val ps = DateTimeScheme("dtg", 2, ChronoUnit.HOURS)
      val filter = ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-03T01:55:00.000Z'")
      ko
//      val covering = ps.getSimplifiedFilters(filter)
//      covering must beSome
//      covering.get must haveSize(2)
//      covering.get.map(_.filter) must containTheSameElementsAs(Seq(Filter.INCLUDE, filter))
//      foreach(covering.get)(_.partial must beFalse)
//      foreach(covering.get)(_.partitions.size mustEqual 1)
    }

    "simplify filters with step > 1 and single partition" >> {
      val ps = DateTimeScheme("dtg", 2, ChronoUnit.HOURS)
      val filter = ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-03T01:55:00.000Z'")
//      val simplified = ps.getSimplifiedFilters(filter)
//      simplified must beSome
//      simplified.get must haveSize(1)
//      simplified.get.head.filter mustEqual filter
//      simplified.get.head.partial must beFalse
//      simplified.get.head.partitions mustEqual Seq("2016/216/00")
      ko
    }

    "simplify filters with step > 1 and multiple partitions" >> {
      val ps = DateTimeScheme("dtg", 2, ChronoUnit.HOURS)
      val filter = ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-03T02:55:00.000Z'")
//      val simplified = ps.getSimplifiedFilters(filter)
//      simplified must beSome
//      simplified.get must haveSize(2)
//      simplified.get.map(_.filter) must containTheSameElementsAs(Seq(Filter.INCLUDE, filter))
//      foreach(simplified.get)(_.partial must beFalse)
//      simplified.get.find(_.filter == Filter.INCLUDE).map(_.partitions) must beSome(Seq("2016/216/00"))
//      simplified.get.find(_.filter != Filter.INCLUDE).map(_.partitions) must beSome(Seq("2016/216/02"))
      ko
    }

    "calculate covering filters for partitions" >> {
      forall(Seq(ChronoUnit.HOURS, ChronoUnit.DAYS, ChronoUnit.WEEKS, ChronoUnit.MONTHS, ChronoUnit.YEARS)) { unit =>
        val ps = DateTimeScheme("dtg", 2, unit)
        val partition = ps.getPartition(sf)
//          val start = DateParsing.parse(partition, format.formatter)
//          val end = start.plus(step, format.unit)
//          val expected = ECQL.toFilter(s"dtg >= '${DateParsing.format(start)}' AND dtg < '${DateParsing.format(end)}'")
//          val covering = ps.getCoveringFilter(partition)
//          decomposeAnd(covering) must containTheSameElementsAs(decomposeAnd(expected))
        ko
      }
    }

    "calculate intersecting partitions for filters" >> {
      forall(Seq(ChronoUnit.HOURS, ChronoUnit.DAYS, ChronoUnit.WEEKS, ChronoUnit.MONTHS, ChronoUnit.YEARS)) { unit =>
        val ps = DateTimeScheme("dtg", 2, unit)
        val partition = ps.getPartition(sf)
//        val start = DateParsing.parse(partition, format.formatter)
//        val end = start.plus(step, format.unit)
//        val filter = ECQL.toFilter(s"dtg >= '${DateParsing.format(start)}' AND dtg < '${DateParsing.format(end)}'")
//        val partitions = ps.getIntersectingPartitions(filter)
//        partitions must beSome(Seq(partition))
        ko
      }
    }

    "handle edge boundaries" >> {
      val dtScheme = DateTimeScheme("dtg", 2, ChronoUnit.DAYS)
      val exclusive = ECQL.toFilter("dtg > '2017-01-02' and dtg < '2017-01-04T00:00:00.000Z'")
//      val twoDays = dtScheme.getSimplifiedFilters(exclusive)
//      twoDays must beSome
//      twoDays.get must haveSize(2)
//      twoDays.get.map(_.filter) must containTheSameElementsAs(Seq(Filter.INCLUDE, exclusive))
//      foreach(twoDays.get)(_.partial must beFalse)
//      twoDays.get.find(_.filter == Filter.INCLUDE).map(_.partitions) must beSome(Seq("2017/20170103"))
//      twoDays.get.find(_.filter != Filter.INCLUDE).map(_.partitions) must beSome(Seq("2017/20170102"))
//      val inclusive = ECQL.toFilter("dtg >= '2017-01-02' and dtg <= '2017-01-04T00:00:00.001Z'")
//      val threeDays = dtScheme.getSimplifiedFilters(inclusive)
//      threeDays must beSome
//      threeDays.get must haveSize(2)
//      threeDays.get.map(_.filter) must containTheSameElementsAs(Seq(Filter.INCLUDE, inclusive))
//      foreach(threeDays.get)(_.partial must beFalse)
//      threeDays.get.find(_.filter == Filter.INCLUDE).map(_.partitions) must beSome(containTheSameElementsAs(Seq("2017/20170102", "2017/20170103")))
//      threeDays.get.find(_.filter != Filter.INCLUDE).map(_.partitions) must beSome(Seq("2017/20170104"))
      ko
    }
  }
}
