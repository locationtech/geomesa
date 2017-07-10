/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.common

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
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
      val ps = new DateTimeScheme("yyyy-MM-dd", ChronoUnit.DAYS, 1, sft, "dtg", true)
      ps.getPartitionName(sf) mustEqual "2017-01-03"
    }

    "partition based on date with slash delimiter" >> {
      val ps = new DateTimeScheme("yyyy/DDD/HH", ChronoUnit.DAYS, 1, sft, "dtg", true)
      ps.getPartitionName(sf) mustEqual "2017/003/10"
    }

    "partition based on date with slash delimiter" >> {
      val ps = new DateTimeScheme("yyyy/DDD/HH", ChronoUnit.DAYS, 1, sft, "dtg", true)
      ps.getPartitionName(sf) mustEqual "2017/003/10"
    }

    "10 bit datetime z2 partition" >> {
      val sf = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

      val sf2 = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(-75, 38))), sft, new FeatureIdImpl("1"))

      val ps = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD", ChronoUnit.DAYS, 1, sft, "dtg", true),
        new Z2Scheme(10, sft, "geom", true)
      ))
      ps.getPartitionName(sf) mustEqual "2017/003/0770"
      ps.getPartitionName(sf2) mustEqual "2017/003/0617"

    }

    "20 bit datetime z2 partition" >> {
      val sf = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

      val sf2 = new SimpleFeatureImpl(
        List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
          gf.createPoint(new Coordinate(-75, 38))), sft, new FeatureIdImpl("1"))

      val ps = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD", ChronoUnit.DAYS, 1, sft, "dtg", true),
        new Z2Scheme(20, sft, "geom", true)
      ))
      ps.getPartitionName(sf) mustEqual "2017/003/0789456"
      ps.getPartitionName(sf2) mustEqual "2017/003/0632516"
    }

    "return correct date partitions" >> {
      val ps = new DateTimeScheme("yyyy/DDD/HH", ChronoUnit.HOURS, 1, sft, "dtg", true)
      val covering = ps.getCoveringPartitions(ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      covering.size() mustEqual 24

    }

    "2 bit datetime z2 partition" >> {
      val ps = new Z2Scheme(2, sft, "geom", true)
      val covering = ps.getCoveringPartitions(ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      covering.size() mustEqual 4
    }

    "2 bit z2 with date" >> {
      val ps = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD/HH", ChronoUnit.HOURS, 1, sft, "dtg", true),
        new Z2Scheme(2, sft, "geom", true)
      ))
      val covering = ps.getCoveringPartitions(ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      covering.size() mustEqual 24 * 4
    }

    "2 bit with filter" >> {
      val ps = new Z2Scheme(2, sft, "geom", true)
      ps.getCoveringPartitions(ECQL.toFilter("bbox(geom, -180, -90, 180, 90, 'EPSG:4326')")).size mustEqual 4
      ps.getCoveringPartitions(ECQL.toFilter("bbox(geom, -1, -1, 1, 1, 'EPSG:4326')")).size mustEqual 4
      ps.getCoveringPartitions(ECQL.toFilter("bbox(geom, -10, 5, 10, 6, 'EPSG:4326')")).size mustEqual 2
    }

    "4 bit with filter" >> {
      val ps = new Z2Scheme(4, sft, "geom", true)
      ps.getCoveringPartitions(ECQL.toFilter("bbox(geom, -180, -90, 180, 90, 'EPSG:4326')")).size mustEqual 16
      ps.getCoveringPartitions(ECQL.toFilter("bbox(geom, -1, -1, 1, 1, 'EPSG:4326')")).size mustEqual 4
      ps.getCoveringPartitions(ECQL.toFilter("bbox(geom, -10, 5, 10, 6, 'EPSG:4326')")).size mustEqual 2
      ps.getCoveringPartitions(ECQL.toFilter("bbox(geom, -90, 5, 90, 6, 'EPSG:4326')")).size mustEqual 3
      ps.getCoveringPartitions(ECQL.toFilter("bbox(geom, -90.000000001, 5, 90, 6, 'EPSG:4326')")).size mustEqual 4
      ps.getCoveringPartitions(ECQL.toFilter("bbox(geom, -90.000000001, 5, 180, 6, 'EPSG:4326')")).size mustEqual 4
    }

    "date time test" >> {
      val ps = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD/HH", ChronoUnit.HOURS, 1, sft, "dtg", true),
        new Z2Scheme(2, sft, "geom", true)
      ))
      val covering = ps.getCoveringPartitions(ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      covering.size mustEqual 96
      // TODO actually test the resulting values...
    }

    "composite scheme test hourly,z2-2bit" >> {
      val ps = CommonSchemeLoader.build("hourly,z2-2bit", sft)
      ps must beAnInstanceOf[CompositeScheme]
      val covering = ps.getCoveringPartitions(ECQL.toFilter("dtg >= '2016-08-03T00:00:00.000Z' and dtg < '2016-08-04T00:00:00.000Z'"))
      covering.size() mustEqual 24 * 4
    }

  }
}
