/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.query

import org.locationtech.jts.geom.{Coordinate, Point}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.GeodeticCalculator
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.accumulo.iterators.TestData
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ProximitySearchProcessTest extends Specification with TestWithMultipleSfts {

  sequential

  val geoFactory = JTSFactoryFinder.getGeometryFactory

  def getPoint(lat: Double, lon: Double, meters: Double): Point = {
    val calc = new GeodeticCalculator()
    calc.setStartingGeographicPoint(lat, lon)
    calc.setDirection(90, meters)
    val dest2D = calc.getDestinationGeographicPoint
    geoFactory.createPoint(new Coordinate(dest2D.getX, dest2D.getY))
  }

  "GeomesaProximityQuery" should {
    "find things close by" in {
      val sft = createNewSchema("*geom:Point:srid=4326,type:String,dtg:Date")
      val sftName = sft.getTypeName
      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          featureCollection.add(sf)
        }
      }

      // write the feature to the store
      val fs = ds.getFeatureSource(sft.getTypeName)
      val res = fs.addFeatures(featureCollection)

      import org.locationtech.geomesa.utils.geotools.Conversions._
      val p1 = getPoint(45, 45, 99)
      WKTUtils.read("POINT(45 45)").bufferMeters(99.1).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(100).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(98).intersects(p1) must beFalse
      val p2 = getPoint(46, 46, 99)
      val p3 = getPoint(47, 47, 99)


      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      List(1, 2, 3).zip(List(p1, p2, p3)).foreach { case (i, p) =>
        val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), i.toString)
        sf.setDefaultGeometry(p)
        sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
        sf.setAttribute("type", "fake")
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        inputFeatures.add(sf)
      }

      val dataFeatures = fs.getFeatures()
      dataFeatures.size should be equalTo 8

      val prox = new ProximitySearchProcess

      // note: size returns an estimated amount, instead we need to actually count the features
      def ex(p: Double) = SelfClosingIterator(prox.execute(inputFeatures, dataFeatures, p)).toSeq

      ex(50.0)  must haveLength(0)
      ex(90.0)  must haveLength(0)
      ex(99.1)  must haveLength(6)
      ex(100.0) must haveLength(6)
      ex(101.0) must haveLength(6)
    }

    "work for a complex case with dates" in {
      // create lineBuffer SFC
      val lineSft = createNewSchema("*geom:LineString:srid=4326,dtg:Date")
      addFeature(lineSft, ScalaSimpleFeature.create(lineSft, "query", "LINESTRING (-45 0, -90 45)", "2014-06-07T12:00:00.000Z"))
      val queryLine = ds.getFeatureSource(lineSft.getTypeName).getFeatures

      // create the data store
      val sftPoints = createNewSchema("*geom:Point:srid=4326,dtg:Date")
      val sftPointsName = sftPoints.getTypeName

      // add the 150 excluded points
      TestData.excludedDwithinPoints.zipWithIndex.foreach{ case (p, i) =>
        addFeature(sftPoints, ScalaSimpleFeature.create(sftPoints, s"exfid$i", p, "2014-06-07T12:00:00.000Z"))
      }

      // add the 50 included points
      TestData.includedDwithinPoints.zipWithIndex.foreach{ case (p, i) =>
        addFeature(sftPoints, ScalaSimpleFeature.create(sftPoints, "infid$i", p, "2014-06-07T12:00:00.000Z"))
      }

      // compose the query
      val during = ECQL.toFilter("dtg DURING 2014-06-07T11:00:00.000Z/2014-06-07T13:00:00.000Z")

      val fs = ds.getFeatureSource(sftPointsName)
      val dataFeatures = fs.getFeatures(during)

      val prox = new ProximitySearchProcess
      // note: size returns an estimated amount, instead we need to actually count the features
      SelfClosingIterator(prox.execute(queryLine, dataFeatures, 150000.0)).toSeq must haveLength(50)
    }
  }

  "GeomesaProximityQuery" should {
    "work on non-accumulo feature sources" in {
      import org.locationtech.geomesa.utils.geotools.Conversions._
      val sftName = "geomesaProximityTestType"
      val sft = SimpleFeatureTypes.createType(sftName, "*geom:Point:srid=4326,type:String,dtg:Date")

      val p1 = getPoint(45, 45, 99)
      WKTUtils.read("POINT(45 45)").bufferMeters(99.1).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(100).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(98).intersects(p1) must beFalse
      val p2 = getPoint(46, 46, 99)
      val p3 = getPoint(47, 47, 99)


      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      List(1, 2, 3).zip(List(p1, p2, p3)).foreach { case (i, p) =>
        val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), i.toString)
        sf.setDefaultGeometry(p)
        sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
        sf.setAttribute("type", "fake")
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        inputFeatures.add(sf)
      }

      val nonAccumulo = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          nonAccumulo.add(sf)
        }
      }

      nonAccumulo.size should be equalTo 8
      val prox = new ProximitySearchProcess
      prox.execute(inputFeatures, nonAccumulo, 30.0).size should be equalTo 0
      prox.execute(inputFeatures, nonAccumulo, 98.0).size should be equalTo 0
      prox.execute(inputFeatures, nonAccumulo, 99.0001).size should be equalTo 6
      prox.execute(inputFeatures, nonAccumulo, 100.0).size should be equalTo 6
      prox.execute(inputFeatures, nonAccumulo, 101.0).size should be equalTo 6
    }
  }

}
