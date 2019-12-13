/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.query

import org.geotools.data.collection.ListFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class RouteSearchProcessTest extends Specification {

  import scala.collection.JavaConversions._

  sequential

  val r = new Random(-10)

  val routeSft = SimpleFeatureTypes.createType("route", "*geom:LineString:srid=4326")
  val sft = SimpleFeatureTypes.createType("tracks", "track:String,heading:Double,dtg:Date,*geom:Point:srid=4326")

  val process = new RouteSearchProcess

  val routes = new ListFeatureCollection(routeSft,
    List(ScalaSimpleFeature.create(routeSft, "r0", "LINESTRING (40 40, 40.5 40.5, 40.5 41)")))

  // features along the lower angled part of the route, headed in the opposite direction
  val features0 = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, s"0$i")
    sf.setAttribute("track", "0")
    sf.setAttribute("heading", Double.box(217.3 + (r.nextDouble * 10) - 5))
    sf.setAttribute("dtg", s"2017-02-20T00:00:0$i.000Z")
    val route = (40.0 + (10 - i) * 0.05) - (r.nextDouble / 100) - 0.005
    sf.setAttribute("geom", s"POINT($route $route)")
    sf
  }

  // features along the upper vertical part of the route
  val features1 = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, s"1$i")
    sf.setAttribute("track", "1")
    sf.setAttribute("heading", Double.box((r.nextDouble * 10) - 5))
    sf.setAttribute("dtg", s"2017-02-20T00:01:0$i.000Z")
    sf.setAttribute("geom", s"POINT(${40.5 + (r.nextDouble / 100) - 0.005} ${40.5 + (i + 1) * 0.005})")
    sf
  }

  // features along the upper vertical part of the route, but with a heading off by 5-15 degrees
  val features2 = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, s"2$i")
    sf.setAttribute("track", "2")
    sf.setAttribute("heading", Double.box(10 + (r.nextDouble * 10) - 5))
    sf.setAttribute("dtg", s"2017-02-20T00:02:0$i.000Z")
    sf.setAttribute("geom", s"POINT(${40.5 + (r.nextDouble / 100) - 0.005} ${40.5 + (i + 1) * 0.005})")
    sf
  }

  // features headed along the upper vertical part of the route, but not close to the route
  val features3 = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, s"3$i")
    sf.setAttribute("track", "3")
    sf.setAttribute("heading", Double.box((r.nextDouble * 10) - 5))
    sf.setAttribute("dtg", s"2017-02-20T00:03:0$i.000Z")
    sf.setAttribute("geom", s"POINT(${40.7 + (r.nextDouble / 10) - 0.005} ${40.5 + (i + 1) * 0.005})")
    sf
  }

  val input = new ListFeatureCollection(sft, features0 ++ features1 ++ features2 ++ features3)

  "RouteSearch" should {
    "return features along a route" in {
      val collection = process.execute(input, routes, 1000.0, 5.0, null, null, false, "heading")

      val results = SelfClosingIterator(collection.features).toSeq
      results must containTheSameElementsAs(features1)
    }

    "return features along a route with a wider heading tolerance" in {
      val collection = process.execute(input, routes, 1000.0, 15.0, null, null, false, "heading")

      val results = SelfClosingIterator(collection.features).toSeq
      results must containTheSameElementsAs(features1 ++ features2)
    }

    "return features along a wide buffered route" in {
      val collection = process.execute(input, routes, 100000.0, 5.0, null, null, false, "heading")

      val results = SelfClosingIterator(collection.features).toSeq
      results must containTheSameElementsAs(features1 ++ features3)
    }

    "return features along a bidirectional route" in {
      val collection = process.execute(input, routes, 1000.0, 5.0, null, null, true, "heading")

      val results = SelfClosingIterator(collection.features).toSeq
      results must containTheSameElementsAs(features0 ++ features1)
    }
  }
}
