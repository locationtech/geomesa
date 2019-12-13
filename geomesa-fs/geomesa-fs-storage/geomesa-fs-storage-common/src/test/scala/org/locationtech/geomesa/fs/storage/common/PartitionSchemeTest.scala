/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.common

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.SimplifiedFilter
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.partitions.{CompositeScheme, DateTimeScheme, XZ2Scheme, Z2Scheme}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class PartitionSchemeTest extends Specification with AllExpectations {

  sequential

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-01-03T10:15:30Z", "POINT (10 10)")

  "PartitionScheme" should {

    "partition based on attribute" >> {
      val ps = PartitionSchemeFactory.load(sft, NamedOptions("attribute", Map("partitioned-attribute" -> "name")))
      ps.getPartitionName(sf) mustEqual "test"
      ps.getSimplifiedFilters(ECQL.toFilter("name IN ('foo', 'bar')")) must
          beSome(Seq(SimplifiedFilter(Filter.INCLUDE, Seq("foo", "bar"), partial = false)))
      ps.getSimplifiedFilters(ECQL.toFilter("name IN ('foo', 'bar')"), Some("foo")) must
          beSome(Seq(SimplifiedFilter(Filter.INCLUDE, Seq("foo"), partial = false)))
      ps.getSimplifiedFilters(ECQL.toFilter("name < 'foo' and name > 'bar'")) must beNone
      ps.getSimplifiedFilters(ECQL.toFilter("bbox(geom,-170,-80,170,80)")) must beNone
    }

    "partition based on date" >> {
      val ps = DateTimeScheme("yyyy-MM-dd", ChronoUnit.DAYS, 1, "dtg", 2)
      ps.getPartitionName(sf) mustEqual "2017-01-03"
    }

    "partition based on date with slash delimiter" >> {
      val ps = DateTimeScheme("yyyy/DDD/HH", ChronoUnit.DAYS, 1, "dtg", 2)
      ps.getPartitionName(sf) mustEqual "2017/003/10"
    }

    "partition based on date with slash delimiter" >> {
      val ps = DateTimeScheme("yyyy/DDD/HH", ChronoUnit.DAYS, 1, "dtg", 2)
      ps.getPartitionName(sf) mustEqual "2017/003/10"
    }

    "weekly partitions" >> {
      val ps = PartitionSchemeFactory.load(sft, NamedOptions("weekly"))
      ps must beAnInstanceOf[DateTimeScheme]
      ps.getPartitionName(sf) mustEqual "2017/01"
      val tenWeeksOut = ScalaSimpleFeature.create(sft, "1", "test", 10,
        Date.from(Instant.parse("2017-01-01T00:00:00Z").plus(9*7 + 1, ChronoUnit.DAYS)), "POINT (10 10)")
      ps.getPartitionName(tenWeeksOut) mustEqual "2017/10"
    }

    "10 bit datetime z2 partition" >> {
      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-01-03T10:15:30Z", "POINT (10 10)")
      val sf2 = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-01-03T10:15:30Z", "POINT (-75 38)")

      val ps = CompositeScheme(Seq(
        DateTimeScheme("yyy/DDD", ChronoUnit.DAYS, 1, "dtg", 2),
        Z2Scheme(10, "geom", 3)
      ))
      ps.getPartitionName(sf) mustEqual "2017/003/0770"
      ps.getPartitionName(sf2) mustEqual "2017/003/0617"

    }

    "10 bit datetime xz2 partition" >> {
      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-01-03T10:15:30Z", "POINT (10 10)")
      val sf2 = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-01-03T10:15:30Z", "POINT (-75 38)")

      val ps = CompositeScheme(Seq(
        DateTimeScheme("yyy/DDD", ChronoUnit.DAYS, 1, "dtg", 2),
        XZ2Scheme(10, "geom", 3)
      ))

      ps.getPartitionName(sf) mustEqual "2017/003/1030"
      ps.getPartitionName(sf2) mustEqual "2017/003/0825"

    }

    "20 bit datetime z2 partition" >> {
      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-01-03T10:15:30Z", "POINT (10 10)")
      val sf2 = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-01-03T10:15:30Z", "POINT (-75 38)")

      val ps = CompositeScheme(Seq(
        DateTimeScheme("yyy/DDD", ChronoUnit.DAYS, 1, "dtg", 2),
        Z2Scheme(20, "geom", 3)
      ))
      ps.getPartitionName(sf) mustEqual "2017/003/0789456"
      ps.getPartitionName(sf2) mustEqual "2017/003/0632516"
    }

    "20 bit datetime xz2 partition" >> {
      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-01-03T10:15:30Z", "POINT (10 10)")
      val sf2 = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-01-03T10:15:30Z", "POINT (-75 38)")

      val ps = CompositeScheme(Seq(
        DateTimeScheme("yyy/DDD", ChronoUnit.DAYS, 1, "dtg", 2),
        XZ2Scheme(20, "geom", 3)
      ))
      ps.getPartitionName(sf) mustEqual "2017/003/1052614"
      ps.getPartitionName(sf2) mustEqual "2017/003/0843360"
    }

    "return correct date partitions" >> {
      val ps = DateTimeScheme("yyyy/DDD/HH", ChronoUnit.HOURS, 1, "dtg", 2)
      val filter = ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-03T01:55:00.000Z'")
      val covering = ps.getSimplifiedFilters(filter)
      covering must beSome
      covering.get must haveSize(2)
      covering.get.map(_.filter) must containTheSameElementsAs(Seq(Filter.INCLUDE, filter))
      foreach(covering.get)(_.partial must beFalse)
      covering.get.find(_.filter == Filter.INCLUDE).map(_.partitions.size) must beSome(1)
      covering.get.find(_.filter != Filter.INCLUDE).map(_.partitions.size) must beSome(1)
    }

    "2 bit datetime z2 partition" >> {
      val ps = Z2Scheme(2, "geom", 3)

      val spatial = ps.getSimplifiedFilters(ECQL.toFilter("bbox(geom,-179,-89,179,89)"))
      spatial must beSome
      spatial.get must haveSize(1)
      spatial.get.head.partial must beFalse
      spatial.get.head.partitions must haveSize(4)

      val temporal = ps.getSimplifiedFilters(
        ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      temporal must beNone
    }

    "2 bit z2 with date" >> {
      val filter = ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'")
      foreach(Seq(
        PartitionSchemeFactory.load(sft, NamedOptions("hourly,z2-2bit")),
        CompositeScheme(Seq(DateTimeScheme("yyy/DDD/HH", ChronoUnit.HOURS, 1, "dtg", 2), Z2Scheme(2, "geom", 3)))
      )) { ps =>
        val covering = ps.getSimplifiedFilters(filter)
        covering must beSome
        covering.get must haveSize(1)
        covering.get.head.filter mustEqual Filter.INCLUDE
        covering.get.head.partial must beTrue
        covering.get.head.partitions must haveSize(24)
      }
    }

    "2 bit with filter" >> {
      val ps = Z2Scheme(2, "geom", 3)
      val filters = Seq(
        ("bbox(geom, -180, -90, 180, 90)", 4),
        ("bbox(geom, -1, -1, 1, 1)", 4),
        ("bbox(geom, -10, 5, 10, 6)", 2)
      )
      foreach(filters) { case (filter, count) =>
        val covering = ps.getSimplifiedFilters(ECQL.toFilter(filter))
        covering must beSome
        covering.get must haveSize(1)
        covering.get.head.partial must beFalse
        covering.get.head.partitions must haveSize(count)
      }
    }

    "4 bit with filter" >> {
      val ps = Z2Scheme(4, "geom", 3)
      val filters = Seq(
        ("bbox(geom, -180, -90, 180, 90)", 16),
        ("bbox(geom, -1, -1, 1, 1)", 4),
        ("bbox(geom, -10, 5, 10, 6)", 2),
        ("bbox(geom, -90, 5, 90, 6)", 3),
        ("bbox(geom, -90.000000001, 5, 90, 6)", 4),
        ("bbox(geom, -90.000000001, 5, 180, 6)", 4)
      )
      foreach(filters) { case (filter, count) =>
        val covering = ps.getSimplifiedFilters(ECQL.toFilter(filter))
        covering must beSome
        covering.get must haveSize(1)
        covering.get.head.partial must beFalse
        covering.get.head.partitions must haveSize(count)
      }
    }

    "handle edge boundaries" >> {
      val dtScheme = DateTimeScheme("yyyy/yyyyMMdd", ChronoUnit.DAYS, 1, "dtg", 2)
      val exclusive = ECQL.toFilter("dtg > '2017-01-02' and dtg < '2017-01-04T00:00:00.000Z'")
      val twoDays = dtScheme.getSimplifiedFilters(exclusive)
      twoDays must beSome
      twoDays.get must haveSize(2)
      twoDays.get.map(_.filter) must containTheSameElementsAs(Seq(Filter.INCLUDE, exclusive))
      foreach(twoDays.get)(_.partial must beFalse)
      twoDays.get.find(_.filter == Filter.INCLUDE).map(_.partitions) must beSome(Seq("2017/20170103"))
      twoDays.get.find(_.filter != Filter.INCLUDE).map(_.partitions) must beSome(Seq("2017/20170102"))
      val inclusive = ECQL.toFilter("dtg >= '2017-01-02' and dtg <= '2017-01-04T00:00:00.001Z'")
      val threeDays = dtScheme.getSimplifiedFilters(inclusive)
      threeDays must beSome
      threeDays.get must haveSize(2)
      threeDays.get.map(_.filter) must containTheSameElementsAs(Seq(Filter.INCLUDE, inclusive))
      foreach(threeDays.get)(_.partial must beFalse)
      threeDays.get.find(_.filter == Filter.INCLUDE).map(_.partitions) must beSome(containTheSameElementsAs(Seq("2017/20170102", "2017/20170103")))
      threeDays.get.find(_.filter != Filter.INCLUDE).map(_.partitions) must beSome(Seq("2017/20170104"))
    }
  }
}
