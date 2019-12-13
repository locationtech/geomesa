/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.TypeInference.{DerivedTransform, LatLon}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TypeInferenceTest extends Specification with LazyLogging {

  import org.locationtech.geomesa.features.serialization.ObjectType._

  val uuidString = "28a12c18-e5ae-4c04-ae7b-bf7cdbfaf234"
  val uuid = java.util.UUID.fromString(uuidString)

  val pointString = "POINT(45 55)"
  val point = WKTUtils.read(pointString)

  val lineStringString = "LINESTRING(-47.28515625 -25.576171875, -48 -26, -49 -27)"
  val lineString = WKTUtils.read(lineStringString)

  val polygonString = "POLYGON((44 24, 44 28, 49 27, 49 23, 44 24))"
  val polygon = WKTUtils.read(polygonString)

  val multiPointString = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"
  val multiPoint = WKTUtils.read(multiPointString)

  val multiLineStringString = "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))"
  val multiLineString = WKTUtils.read(multiLineStringString)

  val multiPolygonString = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))"
  val multiPolygon = WKTUtils.read(multiPolygonString)

  val geometryCollectionString = "GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))"
  val geometryCollection = WKTUtils.read(geometryCollectionString)

  "TypeInference" should {
    "infer simple types" in {
      // note: don't put any valid lat/lon pairs next to each other or it will create a geometry type
      val types = TypeInference.infer(Seq(Seq("a", 1, 200L, 1f, 200d, true)))
      types.map(_.typed) mustEqual Seq(STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN)
    }
    "infer simple types from strings" in {
      // note: don't put any valid lat/lon pairs next to each other or it will create a geometry type
      val types = TypeInference.infer(Seq(Seq("a", "1", s"${Int.MaxValue.toLong + 1L}", "1.1", "true", "1.00000001")))
      types.map(_.typed) mustEqual Seq(STRING, INT, LONG, FLOAT, BOOLEAN, DOUBLE)
    }
    "infer complex types" in {
      val types = TypeInference.infer(Seq(Seq(new Date(), Array[Byte](0), uuid)))
      types.map(_.typed) mustEqual Seq(DATE, BYTES, UUID)
    }
    "infer complex types from strings" in {
      val types = TypeInference.infer(Seq(Seq("2018-01-01T00:00:00.000Z", uuidString)))
      types.map(_.typed) mustEqual Seq(DATE, UUID)
    }
    "infer geometry types" in {
      val types = TypeInference.infer(Seq(Seq(point, lineString, polygon, multiPoint, multiLineString,
        multiPolygon, geometryCollection))).map(_.typed)
      types mustEqual Seq(POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION)
    }
    "infer geometry types from strings" in {
      val types = TypeInference.infer(Seq(Seq(pointString, lineStringString, polygonString, multiPointString,
        multiLineStringString, multiPolygonString, geometryCollectionString))).map(_.typed)
      types mustEqual Seq(POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION)
    }
    "merge up number types" in {
      val types = Seq(Seq(1d), Seq(1f), Seq(1L), Seq(1))
      foreach(types.drop(0).permutations.toSeq)(t => TypeInference.infer(t).map(_.typed) mustEqual Seq(DOUBLE))
      foreach(types.drop(1).permutations.toSeq)(t => TypeInference.infer(t).map(_.typed) mustEqual Seq(FLOAT))
      foreach(types.drop(2).permutations.toSeq)(t => TypeInference.infer(t).map(_.typed) mustEqual Seq(LONG))
    }
    "merge up number types with lat/lon" in {
      TypeInference.infer(Seq(Seq(135), Seq(45))).map(_.typed) mustEqual Seq(INT)
      TypeInference.infer(Seq(Seq(135f), Seq(45f))).map(_.typed) mustEqual Seq(FLOAT)
      TypeInference.infer(Seq(Seq(135d), Seq(45d))).map(_.typed) mustEqual Seq(DOUBLE)
    }
    "merge up geometry types" in {
      val types = Seq(Seq(point), Seq(lineString), Seq(polygon), Seq(multiPoint), Seq(multiLineString),
        Seq(multiPolygon), Seq(geometryCollection))
      foreach(types.permutations.toSeq)(t => TypeInference.infer(t).map(_.typed) mustEqual Seq(GEOMETRY))
    }
    "merge up null values" in {
      val values = Seq("a", 1, 1L, 1f, 1d, true, new Date(), Seq[Byte](0), uuid, point, lineString,
        polygon, multiPoint, multiLineString, multiPolygon, geometryCollection)
      foreach(values) { value =>
        TypeInference.infer(Seq(Seq(value), Seq(null))) mustEqual TypeInference.infer(Seq(Seq(value)))
      }
    }
    "create points from lon/lat pairs" in {
      import LatLon.{Lat, NotLatLon}

      val floats = TypeInference.infer(Seq(Seq(45f, 55f, "foo")))
      floats.map(_.typed) mustEqual Seq(FLOAT, FLOAT, STRING, POINT)
      val doubles = TypeInference.infer(Seq(Seq(45d, 55d, "foo")))
      doubles.map(_.typed) mustEqual Seq(DOUBLE, DOUBLE, STRING, POINT)
      foreach(Seq(floats, doubles)) { types =>
        types.map(_.latlon) mustEqual Seq(Lat, Lat, NotLatLon, NotLatLon)
        types(3).transform mustEqual DerivedTransform("point", types(0).name, types(1).name)
      }
    }
    "create points from lat/lon pairs" in {
      import LatLon.{Lat, Lon, NotLatLon}

      val floats = TypeInference.infer(Seq(Seq(45f, 120f, "foo")))
      floats.map(_.typed) mustEqual Seq(FLOAT, FLOAT, STRING, POINT)
      val doubles = TypeInference.infer(Seq(Seq(45d, 120d, "foo")))
      doubles.map(_.typed) mustEqual Seq(DOUBLE, DOUBLE, STRING, POINT)
      foreach(Seq(floats, doubles)) { types =>
        types.map(_.latlon) mustEqual Seq(Lat, Lon, NotLatLon, NotLatLon)
        types(3).transform mustEqual DerivedTransform("point", types(1).name, types(0).name)
      }
    }
    "create points from named lat/lon fields" in {
      import LatLon.{Lat, Lon, NotLatLon}

      val floats = TypeInference.infer(Seq(Seq(45f, 120f, 121f, "foo")), Seq("lat", "bar", "lon", "foo"))
      floats.map(_.typed) mustEqual Seq(FLOAT, FLOAT, FLOAT, STRING, POINT)
      val doubles = TypeInference.infer(Seq(Seq(45d, 120d, 121d, "foo")), Seq("lat", "bar", "lon", "foo"))
      doubles.map(_.typed) mustEqual Seq(DOUBLE, DOUBLE, DOUBLE, STRING, POINT)
      foreach(Seq(floats, doubles)) { types =>
        types.map(_.latlon) mustEqual Seq(Lat, Lon, Lon, NotLatLon, NotLatLon)
        types(4).transform mustEqual DerivedTransform("point", types(2).name, types(0).name)
      }
    }
    "not create points from unpaired numbers" in {
      TypeInference.infer(Seq(Seq(45f, "foo", 55f))).map(_.typed) mustEqual Seq(FLOAT, STRING, FLOAT)
      TypeInference.infer(Seq(Seq(45d, "foo", 55d))).map(_.typed) mustEqual Seq(DOUBLE, STRING, DOUBLE)
    }
    "not create points if another geometry is present" in {
      TypeInference.infer(Seq(Seq(45f, 55f, "POINT (40 50)"))).map(_.typed) mustEqual Seq(FLOAT, FLOAT, POINT)
      TypeInference.infer(Seq(Seq(45d, 55d, "POINT (40 50)"))).map(_.typed) mustEqual Seq(DOUBLE, DOUBLE, POINT)
    }
    "not create points if values are not valid lat/lons" in {
      TypeInference.infer(Seq(Seq(145f, 155f, "foo"))).map(_.typed) mustEqual Seq(FLOAT, FLOAT, STRING)
      TypeInference.infer(Seq(Seq(145d, 155d, "foo"))).map(_.typed) mustEqual Seq(DOUBLE, DOUBLE, STRING)
    }
    "infer types despite some failures" in {
      TypeInference.infer(Seq.tabulate(11)(i => Seq(i)) :+ Seq("foo")).map(_.typed) mustEqual Seq(INT)
      TypeInference.infer(Seq.tabulate(11)(i => Seq(i)) :+ Seq("foo"), failureRate = 0.01f).map(_.typed) mustEqual Seq(STRING)
    }
    "fall back to string type" in {
      TypeInference.infer(Seq(Seq("2018-01-01"), Seq(uuidString))).map(_.typed) mustEqual Seq(STRING)
    }
  }
}
