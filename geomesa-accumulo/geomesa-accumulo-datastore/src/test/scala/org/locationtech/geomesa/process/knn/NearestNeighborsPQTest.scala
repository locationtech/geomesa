/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.knn

import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class NearestNeighborsPQTest extends Specification {

  val sftName = "geomesaKNNTestQueryFeature"
  val sft = SimpleFeatureTypes.createType(sftName, "geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date;geomesa.mixed.geometries=true")

  val equatorSF = SimpleFeatureBuilder.build(sft, List(), "equator")
  equatorSF.setDefaultGeometry(WKTUtils.read(f"POINT(0.1 0.2)"))
  equatorSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

  val midpointSF = SimpleFeatureBuilder.build(sft, List(), "midpoint")
  midpointSF.setDefaultGeometry(WKTUtils.read(f"POINT(45.1 45.1)"))
  midpointSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

  val polarSF = SimpleFeatureBuilder.build(sft, List(), "polar")
  polarSF.setDefaultGeometry(WKTUtils.read(f"POINT(89.9 89.9)"))
  polarSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

  val polarSF2 = SimpleFeatureBuilder.build(sft, List(), "polar2")
  polarSF2.setDefaultGeometry(WKTUtils.read(f"POINT(0.0001 89.9)"))
  polarSF2.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

  val lineSF = SimpleFeatureBuilder.build(sft, List(), "line")
  lineSF.setDefaultGeometry(WKTUtils.read(f"LINESTRING(45.0 45.0, 50.0 50.0 )"))
  lineSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

  def diagonalFeatureCollection: DefaultFeatureCollection = {
    val sftName = "geomesaKNNTestDiagonalFeature"
    val sft = SimpleFeatureTypes.createType(sftName, "geom:Point:srid=4326,dtg:Date,dtg_end_time:Date")
    sft.getUserData()(SimpleFeatureTypes.Configs.DEFAULT_DATE_KEY) = "dtg"

    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    // generate a range of Simple Features along a "diagonal"
    Range(0, 91).foreach { lat =>
      val sf = SimpleFeatureBuilder.build(sft, List(), lat.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
    featureCollection
  }

  def polarFeatureCollection: DefaultFeatureCollection = {
    val sftName = "geomesaKNNTestPolarFeature"
    val sft = SimpleFeatureTypes.createType(sftName, "geom:Point:srid=4326,dtg:Date,dtg_end_time:Date")
    sft.getUserData()(SimpleFeatureTypes.Configs.DEFAULT_DATE_KEY) = "dtg"

    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    val polarLat = 89.9
    // generate a range of Simple Features along a "diagonal"
    Range(-180, 180).foreach { lon =>
      val sf = SimpleFeatureBuilder.build(sft, List(), lon.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lon%d $polarLat)"))
      sf.setAttribute("dtg", "2011-01-01T00:00:00Z")
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
    featureCollection
  }


  "Geomesa NearestNeighbor PriorityQueue" should {
    "find things close by the equator" in {
      val equatorPQ = NearestNeighbors(equatorSF, 10)

      val sfWDC = SelfClosingIterator(diagonalFeatureCollection.features).map {
        sf=> SimpleFeatureWithDistance(sf,equatorPQ.distance(sf))
      }.toList

      equatorPQ.add(sfWDC)

      equatorPQ.getK.head.sf.getID must equalTo("0")
    }

    "find things close by Southwest Russia" in {
      val midpointPQ = NearestNeighbors(midpointSF, 10)

      val sfWDC = SelfClosingIterator(diagonalFeatureCollection.features).map {
        sf=> SimpleFeatureWithDistance(sf,midpointPQ.distance(sf))
      }.toList

      midpointPQ.add(sfWDC)

      midpointPQ.getK.head.sf.getID must equalTo("45")
    }

    "find things close by the North Pole" in {
      val polarPQ = NearestNeighbors(polarSF, 10)

      val sfWDC =  SelfClosingIterator(diagonalFeatureCollection.features).map{
        sf=> SimpleFeatureWithDistance(sf,polarPQ.distance(sf))
      }.toList

      polarPQ.add(sfWDC)

      polarPQ.getK.head.sf.getID must equalTo("90")
    }

    "find things in the north polar region" in {
      val polarPQ = NearestNeighbors(polarSF, 10)

      val sfWDC = SelfClosingIterator(polarFeatureCollection.features).map {
        sf=> SimpleFeatureWithDistance(sf,polarPQ.distance(sf))
      }.toList

      polarPQ.add(sfWDC)

      polarPQ.getK.head.sf.getID must equalTo("90")
    }

    "find more things near the north polar region" in {
      val polarPQ = NearestNeighbors(polarSF2, 10)

      val sfWDC = SelfClosingIterator(polarFeatureCollection.features).map {
        sf => SimpleFeatureWithDistance(sf, polarPQ.distance(sf))
      }.toList

      polarPQ.add(sfWDC)

      polarPQ.getK.head.sf.getID must equalTo("0")
    }

    "ignore extra features that are too far away" in {
      val polarPQ = NearestNeighbors(polarSF2, 10)

      val polarSFWDC = SelfClosingIterator(polarFeatureCollection.features).map {
        sf => SimpleFeatureWithDistance(sf, polarPQ.distance(sf))
      }.toList

      val dSFWDC = SelfClosingIterator(diagonalFeatureCollection.features).map {
        sf => SimpleFeatureWithDistance(sf, polarPQ.distance(sf))
      }.toList

      polarPQ.add(polarSFWDC)
      polarPQ.add(dSFWDC)

      polarPQ.getK.head.sf.getID must equalTo("0")
    }

    "should produce the same results as its clone" in {

      val polarPQ = NearestNeighbors(polarSF2, 10)

      val sfWDC = SelfClosingIterator(polarFeatureCollection.features).map {
        sf => SimpleFeatureWithDistance(sf, polarPQ.distance(sf))
      }.toList

      polarPQ.add(sfWDC)

      val clonePQ = polarPQ.clone()
      polarPQ.getK.map{_.sf.getID} must equalTo(clonePQ.getK.map{_.sf.getID})
    }

    "should produce the same results as getKNN" in {
      val polarPQ = NearestNeighbors(polarSF2, 10)

      val sfWDC = SelfClosingIterator(polarFeatureCollection.features).map {
        sf => SimpleFeatureWithDistance(sf, polarPQ.distance(sf))
      }.toList

      polarPQ.add(sfWDC)

      polarPQ.getK.map{_.sf.getID} must equalTo(polarPQ.getKNN.getK.map{_.sf.getID})
    }

    "thrown an exception when given non-point geometries" in {
        NearestNeighbors(lineSF ,10) should throwAn[RuntimeException]
    }
  }
}
