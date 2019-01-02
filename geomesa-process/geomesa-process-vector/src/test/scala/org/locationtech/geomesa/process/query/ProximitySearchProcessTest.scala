/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.query

import org.locationtech.jts.geom.{Coordinate, Point}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.GeodeticCalculator
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProximitySearchProcessTest extends Specification {

  val geoFactory = JTSFactoryFinder.getGeometryFactory

  def getPoint(lat: Double, lon: Double, meters: Double): Point = {
    val calc = new GeodeticCalculator()
    calc.setStartingGeographicPoint(lat, lon)
    calc.setDirection(90, meters)
    val dest2D = calc.getDestinationGeographicPoint
    geoFactory.createPoint(new Coordinate(dest2D.getX, dest2D.getY))
  }

  "ProximitySearchProcess" should {
    "manually visit a feature collection" in {
      import org.locationtech.geomesa.utils.geotools.Conversions._

      val sftName = "proximity"
      val sft = SimpleFeatureTypes.createType(sftName, "*geom:Point:srid=4326,type:String,dtg:Date")

      val dataFeatures = new ListFeatureCollection(sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = new ScalaSimpleFeature(sft, name + i)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
          sf.setAttribute("type", name)
          dataFeatures.add(sf)
        }
      }
      dataFeatures.size mustEqual 8

      val p1 = getPoint(45, 45, 99)
      val p2 = getPoint(46, 46, 99)
      val p3 = getPoint(47, 47, 99)

      WKTUtils.read("POINT(45 45)").bufferMeters(99.1).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(100).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(98).intersects(p1) must beFalse

      val inputFeatures = new ListFeatureCollection(sft)
      List(1, 2, 3).zip(List(p1, p2, p3)).foreach { case (i, p) =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.setDefaultGeometry(p)
        sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
        sf.setAttribute("type", "fake")
        inputFeatures.add(sf)
      }

      val prox = new ProximitySearchProcess

      // note: size returns an estimated amount, instead we need to actually count the features
      def ex(p: Double) = SelfClosingIterator(prox.execute(inputFeatures, dataFeatures, p)).toSeq

      ex(50.0)  must haveLength(0)
      ex(90.0)  must haveLength(0)
      ex(99.1)  must haveLength(6)
      ex(100.0) must haveLength(6)
      ex(101.0) must haveLength(6)
    }
  }
}
