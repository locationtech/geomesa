/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.uuid

import java.util.Date

import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.{BinnedTime, TimePeriod}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.{Geometry, Point, Polygon}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

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
    builder.addAll(Array[Object](geom, dtg.asInstanceOf[Object], name))
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
