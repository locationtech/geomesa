/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.PartitionRange
import org.locationtech.geomesa.fs.storage.api.PartitionSchemeFactory
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.DateParsing
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.time.temporal.{ChronoField, ChronoUnit}
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.Date

@RunWith(classOf[JUnitRunner])
class DateTimeSchemeTest extends Specification {

  import org.locationtech.geomesa.filter.decomposeAnd

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-02-03T10:15:30Z", "POINT (10 10)")
  val date = ZonedDateTime.ofInstant(sf.getAttribute(2).asInstanceOf[Date].toInstant, ZoneOffset.UTC)

  val epoch = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)

  "DateTimeScheme" should {

    "partition based on days" >> {
      val ps = DateTimeScheme("dtg", 2, ChronoUnit.DAYS)
      val partition = ps.getPartition(sf)
      partition mustEqual "80004330"
      val days = AttributeIndexKey.decode("integer", partition).asInstanceOf[Int]
      ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC).plusDays(days) mustEqual truncate(date, ChronoUnit.DAYS)
    }

    "partition based on hours" >> {
      val ps = DateTimeScheme("dtg", 2, ChronoUnit.HOURS)
      val partition = ps.getPartition(sf)
      partition mustEqual "80064c8a"
      val hours = AttributeIndexKey.decode("integer", partition).asInstanceOf[Int]
      ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC).plusHours(hours) mustEqual truncate(date, ChronoUnit.HOURS)
    }

    "partition based on week" >> {
      val ps = PartitionSchemeFactory.load(sft, "weekly")
      ps must beAnInstanceOf[DateTimeScheme]
      val partition = ps.getPartition(sf)
      partition mustEqual "80000999"
      val weeks = AttributeIndexKey.decode("integer", partition).asInstanceOf[Int]
      ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC).plusWeeks(weeks) mustEqual truncate(date, ChronoUnit.WEEKS)
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
    }.pendingUntilFixed("not implemented")

    "simplify filters with multiple partitions" >> {
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
    }.pendingUntilFixed("not implemented")

    "calculate covering filters for partitions" >> {
      foreach(Seq(ChronoUnit.HOURS, ChronoUnit.DAYS, ChronoUnit.WEEKS, ChronoUnit.MONTHS, ChronoUnit.YEARS)) { unit =>
        val ps = DateTimeScheme("dtg", 2, unit)
        val partition = ps.getPartition(sf)
        val covering = ps.getCoveringFilter(partition)
        val expected = {
          val start = truncate(date, unit)
          val end = start.plus(1, unit)
          ECQL.toFilter(s"dtg >= '${DateParsing.format(start)}' AND dtg < '${DateParsing.format(end)}'")
        }
        decomposeAnd(covering) must containTheSameElementsAs(decomposeAnd(expected))
      }
    }

    "calculate intersecting partitions for filters" >> {
      foreach(Seq(ChronoUnit.HOURS, ChronoUnit.DAYS, ChronoUnit.WEEKS, ChronoUnit.MONTHS, ChronoUnit.YEARS)) { unit =>
        val ps = DateTimeScheme("dtg", 2, unit)
        val partition = ps.getPartition(sf)
        val expectedEndPartition = java.lang.Long.toHexString(java.lang.Long.parseLong(partition, 16) + 1)
        val start = truncate(date, unit)
        val end = start.plus(1, unit)
        val filter = ECQL.toFilter(s"dtg >= '${DateParsing.format(start)}' AND dtg < '${DateParsing.format(end)}'")
        val partitions = ps.getIntersectingPartitions(filter).orNull
        partitions must not(beNull)
        partitions must haveLength(1)
        partitions.head.bounds mustEqual Seq(PartitionRange(ps.name, partition, expectedEndPartition))
        partitions.head.filter must beSome(filter) // TODO
      }
    }

    // TODO
//    "handle edge boundaries" >> {
//      val dtScheme = DateTimeScheme("dtg", 2, ChronoUnit.DAYS)
//      val exclusive = ECQL.toFilter("dtg > '2017-01-02' and dtg < '2017-01-04T00:00:00.000Z'")
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
//    }
  }

  private def truncate(date: ZonedDateTime, unit: ChronoUnit): ZonedDateTime = {
    unit match {
      case ChronoUnit.HOURS | ChronoUnit.DAYS => date.truncatedTo(unit)
      case _ => epoch.plus(unit.between(epoch, date), unit)
    }
  }
}
