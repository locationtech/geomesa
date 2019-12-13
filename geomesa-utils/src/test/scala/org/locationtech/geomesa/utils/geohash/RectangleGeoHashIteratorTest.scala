/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import org.junit.runner.RunWith
import org.locationtech.jts.geom.Coordinate
import org.locationtech.geomesa.utils.geohash.GeoHashIterator._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RectangleGeoHashIteratorTest extends Specification {

  "RectangleGeoHashIterator" should {

    "work for corners" >> {
      val bitsPrecision = 35
      val ghBig = GeoHash.apply("9q8ys0")
      val bbox = ghBig.bbox
      val ll = geometryFactory.createPoint(new Coordinate(bbox.ll.getX, bbox.ll.getY))
      val ur = geometryFactory.createPoint(new Coordinate(bbox.ur.getX, bbox.ur.getY))
      val rghi = new RectangleGeoHashIterator(ll.getY, ll.getX,
        ur.getY, ur.getX,
        bitsPrecision)
      rghi must not(beNull)

      val length = rghi.foldLeft(0) { case (count, gh) =>
        gh must not(beNull)
        val pt = geometryFactory.createPoint(new Coordinate(gh.x, gh.y))
        val s = s"${gh.toString} == (${pt.getY},${pt.getX}) <-> (${bbox.ll.getY},${bbox.ll.getX}::${bbox.ur.getY},${bbox.ur.getX})"
        pt.getY must beGreaterThanOrEqualTo(bbox.ll.getY)
        pt.getY must beLessThanOrEqualTo(bbox.ur.getY)
        pt.getX must beGreaterThanOrEqualTo(bbox.ll.getX)
        pt.getX must beLessThanOrEqualTo(bbox.ur.getX)
        count + 1
      }

      length mustEqual 32
    }

    "iterate successfully" >> {
      val longitude = -78.0
      val latitude = 38.0
      val radiusMeters = 500.0
      val bitsPrecision = 35
      val ptCenter = geometryFactory.createPoint(new Coordinate(longitude, latitude))
      val ptLL = VincentyModel.moveWithBearingAndDistance(ptCenter, -135.0, radiusMeters * 707.0 / 500.0)
      val ptUR = VincentyModel.moveWithBearingAndDistance(ptCenter,   45.0, radiusMeters * 707.0 / 500.0)
      val rghi = new RectangleGeoHashIterator(ptLL.getY, ptLL.getX,
        ptUR.getY, ptUR.getX,
        bitsPrecision)
      rghi must not(beNull)
      val ll = geometryFactory.createPoint(new Coordinate(ptLL.getX, ptLL.getY))
      val ur = geometryFactory.createPoint(new Coordinate(ptUR.getX, ptUR.getY))
      val ghLL = GeoHash.apply(ll, bitsPrecision)
      val ghUR = GeoHash.apply(ur, bitsPrecision)
      val list = List(ptLL, ptUR)
      val (llgh, urgh) = GeoHashIterator.getBoundingGeoHashes(list, bitsPrecision, 0.0)
      ghLL.toBinaryString mustEqual llgh.toBinaryString
      ghUR.toBinaryString mustEqual urgh.toBinaryString
      val length = rghi.foldLeft(0) { case (count, gh) =>
        gh must not(beNull)
        count + 1
      }
      length mustEqual 80
    }
  }
}
