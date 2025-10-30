/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.process.query

import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.GeodeticCalculator
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.process.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.process.query.ProximitySearchProcess
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.{Coordinate, Point}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProximitySearchProcessTest extends Specification with TestWithDataStore {

  sequential

  override def spec: String = throw new UnsupportedOperationException()

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
          val sf = ScalaSimpleFeature.create(sft, name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
          sf.setAttribute("type", name)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
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
        val sf = ScalaSimpleFeature.create(sft, i.toString)
        sf.setDefaultGeometry(p)
        sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
        sf.setAttribute("type", "fake")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
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
      addFeature(ScalaSimpleFeature.create(lineSft, "query", "LINESTRING (-45 0, -90 45)", "2014-06-07T12:00:00.000Z"))
      val queryLine = ds.getFeatureSource(lineSft.getTypeName).getFeatures()

      // create the data store
      val sftPoints = createNewSchema("*geom:Point:srid=4326,dtg:Date")
      val sftPointsName = sftPoints.getTypeName

      // add the 150 excluded points
      ProximitySearchProcessTest.excludedDwithinPoints.zipWithIndex.foreach{ case (p, i) =>
        addFeature(ScalaSimpleFeature.create(sftPoints, s"exfid$i", p, "2014-06-07T12:00:00.000Z"))
      }

      // add the 50 included points
      ProximitySearchProcessTest.includedDwithinPoints.zipWithIndex.foreach{ case (p, i) =>
        addFeature(ScalaSimpleFeature.create(sftPoints, "infid$i", p, "2014-06-07T12:00:00.000Z"))
      }

      // compose the query
      val during = ECQL.toFilter("dtg DURING 2014-06-07T11:00:00.000Z/2014-06-07T13:00:00.000Z")

      val fs = ds.getFeatureSource(sftPointsName)
      val dataFeatures = fs.getFeatures(during)

      val prox = new ProximitySearchProcess
      // note: size returns an estimated amount, instead we need to actually count the features
      SelfClosingIterator(prox.execute(queryLine, dataFeatures, 150000.0)).toSeq must haveLength(50)
    }

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
        val sf = ScalaSimpleFeature.create(sft, i.toString)
        sf.setDefaultGeometry(p)
        sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
        sf.setAttribute("type", "fake")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        inputFeatures.add(sf)
      }

      val nonAccumulo = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = ScalaSimpleFeature.create(sft, name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
          sf.setAttribute("type", name)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
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

object ProximitySearchProcessTest {

  // copied from org.locationtech.geomesa.accumulo.iterators.TestData

  // 49 features
  val includedDwithinPoints = Seq(
    "POINT (-69.860982683110791 25.670999804594551)",
    "POINT (-63.997858975742645 18.9943062994308)",
    "POINT (-81.830327678750578 37.085775640526542)",
    "POINT (-89.490770137868509 44.106303328073992)",
    "POINT (-64.863769432654507 20.078089354931279)",
    "POINT (-76.746987331939451 30.754452084293632)",
    "POINT (-83.143545929322613 38.85610727559537)",
    "POINT (-83.492602490558113 37.349478312620306)",
    "POINT (-69.679833785334282 24.388803458126716)",
    "POINT (-77.217346548139218 33.10117253660443)",
    "POINT (-48.624646482440973 4.055888157616433)",
    "POINT (-59.965969544921109 13.73922529393128)",
    "POINT (-69.719571567790766 25.42188199205567)",
    "POINT (-49.861755695550691 5.008207149016378)",
    "POINT (-59.53028948688214 13.666221546357587)",
    "POINT (-78.573811951638518 33.748446969499653)",
    "POINT (-75.148246144186032 29.088689349563502)",
    "POINT (-78.977185458964598 34.508762904115628)",
    "POINT (-45.145757200454497 -0.675717483534498)",
    "POINT (-70.814939235491693 24.670046948143156)",
    "POINT (-63.816714527267649 18.489239068296545)",
    "POINT (-54.20652730539409 9.01394728018499)",
    "POINT (-71.982651812779181 26.781538560326045)",
    "POINT (-51.71074903691521 7.783630450718865)",
    "POINT (-57.277254589777193 11.028044049316886)",
    "POINT (-51.778519694248303 7.700192534033889)",
    "POINT (-54.576171577496979 9.411552717211283)",
    "POINT (-58.018434745348337 13.069053319459581)",
    "POINT (-79.297793388564656 33.297052361806031)",
    "POINT (-54.041752176309622 8.401677730812796)",
    "POINT (-77.022561401567557 31.287987114079616)",
    "POINT (-54.273277144188896 8.423007576210081)",
    "POINT (-82.635439242627612 37.220921020638443)",
    "POINT (-66.240260183377984 22.333298874601866)",
    "POINT (-63.174079891818458 18.590732503333914)",
    "POINT (-49.604756624845336 3.603030252579086)",
    "POINT (-51.052335953192923 7.155692678339275)",
    "POINT (-79.426274623480495 34.457318692249387)",
    "POINT (-50.914821524842488 5.997763902901978)",
    "POINT (-58.002088202256417 13.707130381901802)",
    "POINT (-82.754970200843246 37.225536788891802)",
    "POINT (-58.739640682739136 13.619726121902358)",
    "POINT (-85.512639282423464 40.180830488630278)",
    "POINT (-88.352340439082099 44.029501612210311)",
    "POINT (-71.510589787816485 27.40689166758548)",
    "POINT (-47.028488437877314 2.675396523547844)",
    "POINT (-69.025674259692593 23.367911342771055)",
    "POINT (-67.336206873060874 22.855689772550061)",
    "POINT (-45.821184492445006 1.02615639387446)",
    "POINT (-59.943416863142957 15.425391686851068)"
  )

  // 150 features
  val excludedDwithinPoints = Seq(
    "POINT (-64.280357700776648 45.0)",
    "POINT (-60.606123086601883 45.0)",
    "POINT (-56.931888472427119 45.0)",
    "POINT (-53.257653858252354 45.0)",
    "POINT (-49.583419244077589 45.0)",
    "POINT (-45.909184629902825 45.0)",
    "POINT (-90.0 41.325765385825235)",
    "POINT (-82.651530771650471 41.325765385825235)",
    "POINT (-78.977296157475706 41.325765385825235)",
    "POINT (-75.303061543300942 41.325765385825235)",
    "POINT (-71.628826929126177 41.325765385825235)",
    "POINT (-67.954592314951412 41.325765385825235)",
    "POINT (-64.280357700776648 41.325765385825235)",
    "POINT (-60.606123086601883 41.325765385825235)",
    "POINT (-56.931888472427119 41.325765385825235)",
    "POINT (-53.257653858252354 41.325765385825235)",
    "POINT (-49.583419244077589 41.325765385825235)",
    "POINT (-45.909184629902825 41.325765385825235)",
    "POINT (-90.0 37.651530771650471)",
    "POINT (-86.325765385825235 37.651530771650471)",
    "POINT (-78.977296157475706 37.651530771650471)",
    "POINT (-75.303061543300942 37.651530771650471)",
    "POINT (-71.628826929126177 37.651530771650471)",
    "POINT (-67.954592314951412 37.651530771650471)",
    "POINT (-64.280357700776648 37.651530771650471)",
    "POINT (-60.606123086601883 37.651530771650471)",
    "POINT (-56.931888472427119 37.651530771650471)",
    "POINT (-53.257653858252354 37.651530771650471)",
    "POINT (-49.583419244077589 37.651530771650471)",
    "POINT (-45.909184629902825 37.651530771650471)",
    "POINT (-90.0 33.977296157475706)",
    "POINT (-86.325765385825235 33.977296157475706)",
    "POINT (-82.651530771650471 33.977296157475706)",
    "POINT (-75.303061543300942 33.977296157475706)",
    "POINT (-71.628826929126177 33.977296157475706)",
    "POINT (-67.954592314951412 33.977296157475706)",
    "POINT (-64.280357700776648 33.977296157475706)",
    "POINT (-60.606123086601883 33.977296157475706)",
    "POINT (-56.931888472427119 33.977296157475706)",
    "POINT (-53.257653858252354 33.977296157475706)",
    "POINT (-49.583419244077589 33.977296157475706)",
    "POINT (-45.909184629902825 33.977296157475706)",
    "POINT (-90.0 30.303061543300938)",
    "POINT (-86.325765385825235 30.303061543300938)",
    "POINT (-82.651530771650471 30.303061543300938)",
    "POINT (-78.977296157475706 30.303061543300938)",
    "POINT (-71.628826929126177 30.303061543300938)",
    "POINT (-67.954592314951412 30.303061543300938)",
    "POINT (-64.280357700776648 30.303061543300938)",
    "POINT (-60.606123086601883 30.303061543300938)",
    "POINT (-56.931888472427119 30.303061543300938)",
    "POINT (-53.257653858252354 30.303061543300938)",
    "POINT (-49.583419244077589 30.303061543300938)",
    "POINT (-45.909184629902825 30.303061543300938)",
    "POINT (-90.0 26.62882692912617)",
    "POINT (-86.325765385825235 26.62882692912617)",
    "POINT (-82.651530771650471 26.62882692912617)",
    "POINT (-78.977296157475706 26.62882692912617)",
    "POINT (-75.303061543300942 26.62882692912617)",
    "POINT (-67.954592314951412 26.62882692912617)",
    "POINT (-64.280357700776648 26.62882692912617)",
    "POINT (-60.606123086601883 26.62882692912617)",
    "POINT (-56.931888472427119 26.62882692912617)",
    "POINT (-53.257653858252354 26.62882692912617)",
    "POINT (-49.583419244077589 26.62882692912617)",
    "POINT (-45.909184629902825 26.62882692912617)",
    "POINT (-90.0 22.954592314951402)",
    "POINT (-86.325765385825235 22.954592314951402)",
    "POINT (-82.651530771650471 22.954592314951402)",
    "POINT (-78.977296157475706 22.954592314951402)",
    "POINT (-75.303061543300942 22.954592314951402)",
    "POINT (-71.628826929126177 22.954592314951402)",
    "POINT (-64.280357700776648 22.954592314951402)",
    "POINT (-60.606123086601883 22.954592314951402)",
    "POINT (-56.931888472427119 22.954592314951402)",
    "POINT (-53.257653858252354 22.954592314951402)",
    "POINT (-49.583419244077589 22.954592314951402)",
    "POINT (-45.909184629902825 22.954592314951402)",
    "POINT (-90.0 19.280357700776634)",
    "POINT (-86.325765385825235 19.280357700776634)",
    "POINT (-82.651530771650471 19.280357700776634)",
    "POINT (-78.977296157475706 19.280357700776634)",
    "POINT (-75.303061543300942 19.280357700776634)",
    "POINT (-71.628826929126177 19.280357700776634)",
    "POINT (-67.954592314951412 19.280357700776634)",
    "POINT (-60.606123086601883 19.280357700776634)",
    "POINT (-56.931888472427119 19.280357700776634)",
    "POINT (-53.257653858252354 19.280357700776634)",
    "POINT (-49.583419244077589 19.280357700776634)",
    "POINT (-45.909184629902825 19.280357700776634)",
    "POINT (-90.0 15.606123086601865)",
    "POINT (-86.325765385825235 15.606123086601865)",
    "POINT (-82.651530771650471 15.606123086601865)",
    "POINT (-78.977296157475706 15.606123086601865)",
    "POINT (-75.303061543300942 15.606123086601865)",
    "POINT (-71.628826929126177 15.606123086601865)",
    "POINT (-67.954592314951412 15.606123086601865)",
    "POINT (-64.280357700776648 15.606123086601865)",
    "POINT (-56.931888472427119 15.606123086601865)",
    "POINT (-53.257653858252354 15.606123086601865)",
    "POINT (-49.583419244077589 15.606123086601865)",
    "POINT (-45.909184629902825 15.606123086601865)",
    "POINT (-90.0 11.931888472427097)",
    "POINT (-86.325765385825235 11.931888472427097)",
    "POINT (-82.651530771650471 11.931888472427097)",
    "POINT (-78.977296157475706 11.931888472427097)",
    "POINT (-75.303061543300942 11.931888472427097)",
    "POINT (-71.628826929126177 11.931888472427097)",
    "POINT (-67.954592314951412 11.931888472427097)",
    "POINT (-64.280357700776648 11.931888472427097)",
    "POINT (-60.606123086601883 11.931888472427097)",
    "POINT (-53.257653858252354 11.931888472427097)",
    "POINT (-49.583419244077589 11.931888472427097)",
    "POINT (-45.909184629902825 11.931888472427097)",
    "POINT (-90.0 8.257653858252329)",
    "POINT (-86.325765385825235 8.257653858252329)",
    "POINT (-82.651530771650471 8.257653858252329)",
    "POINT (-78.977296157475706 8.257653858252329)",
    "POINT (-75.303061543300942 8.257653858252329)",
    "POINT (-71.628826929126177 8.257653858252329)",
    "POINT (-67.954592314951412 8.257653858252329)",
    "POINT (-64.280357700776648 8.257653858252329)",
    "POINT (-60.606123086601883 8.257653858252329)",
    "POINT (-56.931888472427119 8.257653858252329)",
    "POINT (-49.583419244077589 8.257653858252329)",
    "POINT (-45.909184629902825 8.257653858252329)",
    "POINT (-90.0 4.583419244077562)",
    "POINT (-86.325765385825235 4.583419244077562)",
    "POINT (-82.651530771650471 4.583419244077562)",
    "POINT (-78.977296157475706 4.583419244077562)",
    "POINT (-75.303061543300942 4.583419244077562)",
    "POINT (-71.628826929126177 4.583419244077562)",
    "POINT (-67.954592314951412 4.583419244077562)",
    "POINT (-64.280357700776648 4.583419244077562)",
    "POINT (-60.606123086601883 4.583419244077562)",
    "POINT (-56.931888472427119 4.583419244077562)",
    "POINT (-53.257653858252354 4.583419244077562)",
    "POINT (-45.909184629902825 4.583419244077562)",
    "POINT (-90.0 0.909184629902795)",
    "POINT (-86.325765385825235 0.909184629902795)",
    "POINT (-82.651530771650471 0.909184629902795)",
    "POINT (-78.977296157475706 0.909184629902795)",
    "POINT (-75.303061543300942 0.909184629902795)",
    "POINT (-71.628826929126177 0.909184629902795)",
    "POINT (-67.954592314951412 0.909184629902795)",
    "POINT (-64.280357700776648 0.909184629902795)",
    "POINT (-60.606123086601883 0.909184629902795)",
    "POINT (-56.931888472427119 0.909184629902795)",
    "POINT (-53.257653858252354 0.909184629902795)",
    "POINT (-49.583419244077589 0.909184629902795)"
  )
}

