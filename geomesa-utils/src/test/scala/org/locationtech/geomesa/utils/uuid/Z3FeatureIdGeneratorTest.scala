/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.uuid

import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.{Geometry, Point, Polygon}
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.TimePeriod._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class Z3FeatureIdGeneratorTest extends Specification {

  sequential

  val point = WKTUtils.read("POINT(-78.1 38.2)").asInstanceOf[Point]

  val polygon = WKTUtils.read("POLYGON((-78.1 38.2, -78.1 39, -78 39, -78 38.2, -78.1 38.2))").asInstanceOf[Polygon]

  val time = 1435598908099L // System.currentTimeMillis()

  val period = Week

  val SFT = SimpleFeatureTypes.createType("dummyTypeName", "*geom:Point:srid=4326,dtg:Date,name:String")

  implicit def promoteLongToDate(value: Long): Date = new Date(value)

  def makeFeature(geom: Geometry, dtg: Date, name: String): SimpleFeature = {
    val builder = new SimpleFeatureBuilder(SFT)
    builder.addAll(Array[Object](geom, dtg.asInstanceOf[Object], name))
    builder.buildFeature("id1")
  }

  "Z3UuidGenerator" should {
    "create uuids with correct formats for a Point" >> {
      val id = Z3UuidGenerator.createUuid(point, time, period).toString
      id.substring(0, 18) mustEqual "109452fb-fd80-4f78"
      val uuid = UUID.fromString(id)
      uuid.version() mustEqual 4
      uuid.variant() mustEqual 2
    }

    "create uuids with correct formats for a Polygon" >> {
      val id = Z3UuidGenerator.createUuid(polygon.asInstanceOf[Geometry], time, period).toString
      id.substring(0, 18) mustEqual "609452fb-fd82-4fe9"
      val uuid = UUID.fromString(id)
      uuid.version() mustEqual 4
      uuid.variant() mustEqual 2
    }

    "return a reasonable UUID without a geometry (Point)" >> {
      Z3UuidGenerator.createUuid(null.asInstanceOf[Point], time, period) must throwA[Exception]
    }

    "return a reasonable UUID without a geometry (Geometry)" >> {
      Z3UuidGenerator.createUuid(null.asInstanceOf[Geometry], time, period) must throwA[Exception]
    }

    "NOT return a reasonable UUID without a time" >> {
      val id = Z3UuidGenerator.createUuid(point, null.asInstanceOf[Long], period).toString
      id must not beNull
    }

    "return a reasonable UUID from a full feature" >> {
      val feature = makeFeature(point, time, "Anonymous")
      Z3UuidGenerator.createUuid(SFT, feature) must not beNull
    }

    "return a reasonable UUID from a feature missing time" >> {
      val feature = makeFeature(point, null, "Anonymous")
      Z3UuidGenerator.createUuid(SFT, feature) must not beNull
    }

    "NOT return a reasonable UUID from a feature missing location" >> {
      val feature = makeFeature(null, time, "Anonymous")
      Z3UuidGenerator.createUuid(SFT, feature) must throwA[Exception]
    }
  }

}
