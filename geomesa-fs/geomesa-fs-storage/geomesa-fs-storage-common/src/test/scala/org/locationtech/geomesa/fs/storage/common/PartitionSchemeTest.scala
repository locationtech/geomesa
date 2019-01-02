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

import org.locationtech.jts.geom.Coordinate
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.common.partitions.{CompositeScheme, DateTimeScheme, XZ2Scheme, Z2Scheme}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class PartitionSchemeTest extends Specification with AllExpectations {

  sequential

  "PartitionScheme" should {
    import scala.collection.JavaConversions._

    val gf = JTSFactoryFinder.getGeometryFactory
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    val sf = new SimpleFeatureImpl(
      List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
        gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

    "partition based on date" >> {
      val ps = new DateTimeScheme("yyyy-MM-dd", ChronoUnit.DAYS, 1, "dtg", true)
      ps.getPartition(sf) mustEqual "2017-01-03"
    }

    "partition based on date with slash delimiter" >> {
      val ps = new DateTimeScheme("yyyy/DDD/HH", ChronoUnit.DAYS, 1, "dtg", true)
      ps.getPartition(sf) mustEqual "2017/003/10"
    }

    "partition based on date with slash delimiter" >> {
      val ps = new DateTimeScheme("yyyy/DDD/HH", ChronoUnit.DAYS, 1, "dtg", true)
      ps.getPartition(sf) mustEqual "2017/003/10"
    }

    "weekly partitions" >> {
      val ps = PartitionScheme.apply(sft, "weekly")
      ps must beAnInstanceOf[DateTimeScheme]
      ps.getPartition(sf) mustEqual "2017/01"
      val tenWeeksOut = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-01T00:00:00Z").plus(9*7 + 1, ChronoUnit.DAYS)),
          gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))
      ps.getPartition(tenWeeksOut) mustEqual "2017/10"
    }

    "10 bit datetime z2 partition" >> {
      val sf = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

      val sf2 = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(-75, 38))), sft, new FeatureIdImpl("1"))

      val ps = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD", ChronoUnit.DAYS, 1, "dtg", true),
        new Z2Scheme(10, "geom", true)
      ))
      ps.getPartition(sf) mustEqual "2017/003/0770"
      ps.getPartition(sf2) mustEqual "2017/003/0617"

    }

    "10 bit datetime xz2 partition" >> {
      val sf = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

      val sf2 = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(-75, 38))), sft, new FeatureIdImpl("1"))

      val ps = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD", ChronoUnit.DAYS, 1, "dtg", true),
        new XZ2Scheme(10, "geom", true)
      ))

      ps.getPartition(sf) mustEqual "2017/003/1030"
      ps.getPartition(sf2) mustEqual "2017/003/0825"

    }

    "20 bit datetime z2 partition" >> {
      val sf = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

      val sf2 = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(-75, 38))), sft, new FeatureIdImpl("1"))

      val ps = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD", ChronoUnit.DAYS, 1, "dtg", true),
        new Z2Scheme(20, "geom", true)
      ))
      ps.getPartition(sf) mustEqual "2017/003/0789456"
      ps.getPartition(sf2) mustEqual "2017/003/0632516"
    }

    "20 bit datetime xz2 partition" >> {
      val sf = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

      val sf2 = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(-75, 38))), sft, new FeatureIdImpl("1"))

      val ps = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD", ChronoUnit.DAYS, 1, "dtg", true),
        new XZ2Scheme(20, "geom", true)
      ))
      ps.getPartition(sf) mustEqual "2017/003/1052614"
      ps.getPartition(sf2) mustEqual "2017/003/0843360"
    }

    "return correct date partitions" >> {
      val ps = new DateTimeScheme("yyyy/DDD/HH", ChronoUnit.HOURS, 1, "dtg", true)
      val covering = ps.getPartitions(ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      covering.size() mustEqual 24
    }

    "2 bit datetime z2 partition" >> {
      val ps = new Z2Scheme(2, "geom", true)
      val covering = ps.getPartitions(ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      covering.size() mustEqual 4
    }

    "2 bit z2 with date" >> {
      val ps = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD/HH", ChronoUnit.HOURS, 1, "dtg", true),
        new Z2Scheme(2, "geom", true)
      ))
      val covering = ps.getPartitions(ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      covering.size() mustEqual 24 * 4
    }

    "2 bit with filter" >> {
      val ps = new Z2Scheme(2, "geom", true)
      ps.getPartitions(ECQL.toFilter("bbox(geom, -180, -90, 180, 90, 'EPSG:4326')")).size mustEqual 4
      ps.getPartitions(ECQL.toFilter("bbox(geom, -1, -1, 1, 1, 'EPSG:4326')")).size mustEqual 4
      ps.getPartitions(ECQL.toFilter("bbox(geom, -10, 5, 10, 6, 'EPSG:4326')")).size mustEqual 2
    }

    "4 bit with filter" >> {
      val ps = new Z2Scheme(4, "geom", true)
      ps.getPartitions(ECQL.toFilter("bbox(geom, -180, -90, 180, 90, 'EPSG:4326')")).size mustEqual 16
      ps.getPartitions(ECQL.toFilter("bbox(geom, -1, -1, 1, 1, 'EPSG:4326')")).size mustEqual 4
      ps.getPartitions(ECQL.toFilter("bbox(geom, -10, 5, 10, 6, 'EPSG:4326')")).size mustEqual 2
      ps.getPartitions(ECQL.toFilter("bbox(geom, -90, 5, 90, 6, 'EPSG:4326')")).size mustEqual 3
      ps.getPartitions(ECQL.toFilter("bbox(geom, -90.000000001, 5, 90, 6, 'EPSG:4326')")).size mustEqual 4
      ps.getPartitions(ECQL.toFilter("bbox(geom, -90.000000001, 5, 180, 6, 'EPSG:4326')")).size mustEqual 4
    }

    "date time test" >> {
      val ps = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD/HH", ChronoUnit.HOURS, 1, "dtg", true),
        new Z2Scheme(2, "geom", true)
      ))
      val covering = ps.getPartitions(ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      covering.size mustEqual 96
      // TODO actually test the resulting values...
    }

    "composite scheme test hourly,z2-2bit" >> {
      val ps = PartitionScheme.apply(sft, "hourly,z2-2bit")
      ps must beAnInstanceOf[CompositeScheme]
      val covering = ps.getPartitions(ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      covering.size() mustEqual 24 * 4
    }

    "handle edge boundaries" >> {
      val dtScheme = new DateTimeScheme("yyyy/yyyyMMdd", ChronoUnit.DAYS, 1, "dtg", true)
      val twoDays = dtScheme.getPartitions(ECQL.toFilter("dtg > '2017-01-02' and dtg < '2017-01-04T00:00:00.000Z'"))
      twoDays.size mustEqual 2
      twoDays.toSeq must containTheSameElementsAs((2 to 3).map(i => f"2017/201701$i%02d"))
      val threeDays = dtScheme.getPartitions(ECQL.toFilter("dtg >= '2017-01-02' and dtg <= '2017-01-04T00:00:00.001Z'"))
      threeDays.size mustEqual 3
      threeDays.toSeq must containTheSameElementsAs((2 to 4).map(i => f"2017/201701$i%02d"))
    }
  }
}
