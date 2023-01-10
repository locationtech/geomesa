/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.uuid

<<<<<<< HEAD
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
=======
<<<<<<< HEAD:geomesa-utils-parent/geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/uuid/Z3FeatureIdGeneratorTest.scala
<<<<<<< HEAD:geomesa-utils-parent/geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/uuid/Z3FeatureIdGeneratorTest.scala
<<<<<<< HEAD:geomesa-utils-parent/geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/uuid/Z3FeatureIdGeneratorTest.scala
=======
import java.util.Date

>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777)):geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/uuid/Z3FeatureIdGeneratorTest.scala
<<<<<<< HEAD
>>>>>>> 69a1e5094b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
=======
import java.util.Date

>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777)):geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/uuid/Z3FeatureIdGeneratorTest.scala
<<<<<<< HEAD
>>>>>>> 276558f47d3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support):geomesa-utils/src/test/scala/org/locationtech/geomesa/utils/uuid/Z3FeatureIdGeneratorTest.scala
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.{BinnedTime, TimePeriod}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.{Geometry, Point, Polygon}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Date

@RunWith(classOf[JUnitRunner])
class Z3FeatureIdGeneratorTest extends Specification {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  sequential

  val point = WKTUtils.read("POINT(-78.1 38.2)").asInstanceOf[Point]

  val polygon = WKTUtils.read("POLYGON((-78.1 38.2, -78.1 39, -78 39, -78 38.2, -78.1 38.2))").asInstanceOf[Polygon]

  val time = 1435598908099L // System.currentTimeMillis()

  val pointSft = SimpleFeatureTypes.createType("dummyTypeName", "*geom:Point:srid=4326,dtg:Date,name:String")
  val polySft = SimpleFeatureTypes.createType("dummyTypeName", "*geom:Polygon:srid=4326,dtg:Date,name:String")

  def makeFeature(sft: SimpleFeatureType, geom: Geometry, dtg: Date, name: String): SimpleFeature = {
    val builder = new SimpleFeatureBuilder(sft)
    builder.addAll(geom, dtg, name)
    builder.buildFeature("id1")
  }

  "Z3UuidGenerator" should {
    "create uuids with correct formats for a Point" >> {
      val id = Z3UuidGenerator.createUuid(point, time, TimePeriod.Week)
      id.toString.substring(0, 18) mustEqual "e0945639-5c84-4f5c"
      id.version() mustEqual 4
      id.variant() mustEqual 2
    }

    "create uuids with correct formats for a Polygon" >> {
      val id = Z3UuidGenerator.createUuid(polygon, time, TimePeriod.Week)
      id.toString.substring(0, 18) mustEqual "90945639-5c86-4fcd"
      id.version() mustEqual 4
      id.variant() mustEqual 2
    }

    "throw an exception on null point" >> {
      Z3UuidGenerator.createUuid(null: Point, time, TimePeriod.Week) must throwAn[IllegalArgumentException]
    }

    "throw an exception on null geometry" >> {
      Z3UuidGenerator.createUuid(null: Geometry, time, TimePeriod.Week) must throwAn[IllegalArgumentException]
    }

    "return a reasonable UUID from a full feature" >> {
      val feature = makeFeature(pointSft, point, new Date(time), "")
      Z3UuidGenerator.createUuid(pointSft, feature) must not(beNull)
    }

    "return a reasonable UUID from a feature missing time" >> {
      val feature = makeFeature(pointSft, point, null, "")
      Z3UuidGenerator.createUuid(pointSft, feature) must not(beNull)
    }

    "return a reasonable UUID from a feature with an empty geometry" >> {
      foreach(Seq(("POINT EMPTY", pointSft), ("POLYGON EMPTY", polySft))) { case (wkt, sft) =>
        val feature = makeFeature(sft, WKTUtils.read(wkt), new Date(time), "")
        Z3UuidGenerator.createUuid(sft, feature) must not(beNull)
      }
    }

    "NOT return a reasonable UUID from a feature missing location" >> {
      foreach(Seq(pointSft, polySft)) { sft =>
        val feature = makeFeature(sft, null, new Date(time), "")
        Z3UuidGenerator.createUuid(sft, feature) must throwAn[IllegalArgumentException]
      }
    }

    "recover the time bin from the UUID" >> {
      val uuid = Z3UuidGenerator.createUuid(pointSft, makeFeature(pointSft, point, new Date(time), ""))
      val bin = BinnedTime.timeToBinnedTime(pointSft.getZ3Interval).apply(time).bin
      val uuidBytes = ByteArrays.uuidToBytes(uuid.getMostSignificantBits, uuid.getLeastSignificantBits)
      Z3UuidGenerator.timeBin(uuidBytes) mustEqual bin
    }
  }
}
