/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.util

import org.locationtech.jts.geom._
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.jts._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataFrameGeoJSONTest extends Specification with TestEnvironment {

  "Row to GeoJSON converter" should {
    val geomFactory = new GeometryFactory()

    "convert points" >> {
      import spark.implicits._
      val jsonParser = new JSONParser()

      val point = geomFactory.createPoint(new Coordinate(1, 2))

      val df = spark.sparkContext.parallelize(Seq((1, point))).toDF("name", "geom")

      import org.locationtech.geomesa.spark.jts.util.GeoJSONExtensions._
      val res = df.toGeoJSON.head()

      val expectedString =
        """ {"type": "Feature",
          | "geometry": {"type": "Point", "coordinates": [1, 2] },
          | "properties": { "name": "1" }
          | }
        """.stripMargin.replaceAll("\\s+", "")
      val expectedJson = jsonParser.parse(expectedString)

      res.replaceAll("\\s+", "") mustEqual expectedString
      jsonParser.parse(res) mustEqual expectedJson
    }

    "convert polygons" >> {
      import spark.implicits._
      val jsonParser = new JSONParser()

      val env = new Envelope(1, 2, 1, 2)
      val polygon = geomFactory.toGeometry(env)
      val df = spark.sparkContext.parallelize(Seq((1, polygon))).toDF("name", "geom")

      import org.locationtech.geomesa.spark.jts.util.GeoJSONExtensions._
      val res = df.toGeoJSON.head().replaceAll("\\s+", "")

      val expectedString =
        """ {"type": "Feature",
          | "geometry": {"type": "Polygon", "coordinates": [[[1,1],[1,2],[2,2],[2,1],[1,1]]] },
          | "properties": { "name": "1" }
          | }
        """.stripMargin.replaceAll("\\s+", "")
      val expectedJSON = jsonParser.parse(expectedString)

      res mustEqual expectedString
      jsonParser.parse(res) mustEqual expectedJSON
    }

    "convert multipolygons" >> {
      import spark.implicits._
      val jsonParser = new JSONParser()

      val envA = new Envelope(1, 2, 1, 2)
      val polygonA = geomFactory.toGeometry(envA).asInstanceOf[Polygon]
      val envB = new Envelope(2, 3, 2, 3)
      val polygonB = geomFactory.toGeometry(envB).asInstanceOf[Polygon]
      val multipoly = geomFactory.createMultiPolygon(Array(polygonA, polygonB))

      val df = spark.sparkContext.parallelize(Seq((1, multipoly))).toDF("name", "geom")

      import org.locationtech.geomesa.spark.jts.util.GeoJSONExtensions._
      val res = df.toGeoJSON.head()

      val geom = jsonParser.parse(res).asInstanceOf[JSONObject].get("geometry")
      geom mustEqual jsonParser.parse("""{"type":"MultiPolygon","coordinates":[[[[1,1],[1,2],[2,2],[2,1],[1,1]]],[[[2,2],[2,3],[3,3],[3,2],[2,2]]]]}""")

    }

    "use geometry if specified" >> {
      import spark.implicits._
      val jsonParser = new JSONParser()

      val env = new Envelope(1, 2, 1, 2)
      val polygon = geomFactory.toGeometry(env)
      val point = geomFactory.createPoint(new Coordinate(1, 2))

      val df = spark.sparkContext.parallelize(Seq((1, polygon, point))).toDF("name", "poly", "point")

      import org.locationtech.geomesa.spark.jts.util.GeoJSONExtensions._
      val res = df.toGeoJSON(2).head()

      val geom = jsonParser.parse(res.replaceAll("\\s+", "")).asInstanceOf[JSONObject].get("geometry")
      geom mustEqual jsonParser.parse("""{"type": "Point", "coordinates": [1, 2] }""")
    }

    "handle multiple rows" >> {
      import spark.implicits._
      val jsonParser = new JSONParser()

      val pointA = geomFactory.createPoint(new Coordinate(1, 1))
      val pointB = geomFactory.createPoint(new Coordinate(2, 2))
      val pointC = geomFactory.createPoint(new Coordinate(3, 3))

      val df = spark.sparkContext.parallelize(Seq((1, pointA), (2, pointB), (3, pointC))).toDF("name", "geom")

      import org.locationtech.geomesa.spark.jts.util.GeoJSONExtensions._
      val res = df.toGeoJSON.collect

      val geoms = res.map(geojson => jsonParser.parse(geojson).asInstanceOf[JSONObject].get("geometry"))
      geoms(0) mustEqual jsonParser.parse("""{"type": "Point", "coordinates": [1, 1] }""")
      geoms(1) mustEqual jsonParser.parse("""{"type": "Point", "coordinates": [2, 2] }""")
      geoms(2) mustEqual jsonParser.parse("""{"type": "Point", "coordinates": [3, 3] }""")
    }

    "handle rows with nulls" >> {
      import spark.implicits._
      val jsonParser = new JSONParser()

      val pointA = geomFactory.createPoint(new Coordinate(1, 1))
      val pointC = geomFactory.createPoint(new Coordinate(3, 3))

      val df = spark.sparkContext.parallelize(Seq((1, pointA), (2, null), (3, pointC))).toDF("name", "geom")

      import org.locationtech.geomesa.spark.jts.util.GeoJSONExtensions._
      val res = df.toGeoJSON.collect

      val geoms = res.map(geojson => jsonParser.parse(geojson).asInstanceOf[JSONObject].get("geometry"))
      geoms(0) mustEqual jsonParser.parse("""{"type": "Point", "coordinates": [1, 1] }""")
      geoms(1) mustEqual jsonParser.parse("null")
      geoms(2) mustEqual jsonParser.parse("""{"type": "Point", "coordinates": [3, 3] }""")
    }
  }
}
