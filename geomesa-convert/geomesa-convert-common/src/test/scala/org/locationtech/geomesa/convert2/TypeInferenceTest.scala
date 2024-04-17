/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.TypeInference.{DerivedTransform, InferredType, PathWithValues}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Date

@RunWith(classOf[JUnitRunner])
class TypeInferenceTest extends Specification with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.ObjectType._

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

  private def inferRow(row: Seq[Any]): Seq[InferredType] =
    TypeInference.infer(row.map(v => PathWithValues("", Seq(v))), Left("")).types.map(_.inferredType)

  private def inferRowWithNames(row: Seq[(String, Any)]): Seq[InferredType] =
    TypeInference.infer(row.map(v => PathWithValues(v._1, Seq(v._2))), Left("")).types.map(_.inferredType)

  private def inferRowTypes(row: Seq[Any]): Seq[ObjectType] =
    TypeInference.infer(row.map(v => PathWithValues("", Seq(v))), Left("")).types.map(_.inferredType.typed)

  private def inferColType(values: Seq[Any], failureRate: Option[Float] = None): ObjectType = {
    val in = Seq(PathWithValues("", values))
    val res = failureRate match {
      case None => TypeInference.infer(in, Left(""))
      case Some(r) => TypeInference.infer(in, Left(""), failureRate = r)
    }
    res.types.head.inferredType.typed
  }

  "TypeInference" should {
    "infer simple types" in {
      // note: don't put any valid lat/lon pairs next to each other or it will create a geometry type
      val types = inferRowTypes(Seq("a", 1, 200L, 1f, 200d, true))
      types mustEqual Seq(STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN)
    }
    "infer simple types from strings" in {
      // note: don't put any valid lat/lon pairs next to each other or it will create a geometry type
      val types = inferRowTypes(Seq("a", "1", s"${Int.MaxValue.toLong + 1L}", "1.1", "true", "1.00000001"))
      types mustEqual Seq(STRING, INT, LONG, DOUBLE, BOOLEAN, DOUBLE)
    }
    "infer complex types" in {
      val types = inferRowTypes(Seq(new Date(), Array[Byte](0), uuid))
      types mustEqual Seq(DATE, BYTES, UUID)
    }
    "infer complex types from strings" in {
      val types = inferRowTypes(Seq("2018-01-01T00:00:00.000Z", uuidString))
      types mustEqual Seq(DATE, UUID)
    }
    "infer geometry types" in {
      val types = inferRowTypes(Seq(point, lineString, polygon, multiPoint, multiLineString, multiPolygon, geometryCollection))
      types mustEqual Seq(POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION)
    }
    "infer geometry types from strings" in {
      val types = inferRowTypes(Seq(pointString, lineStringString, polygonString, multiPointString,
        multiLineStringString, multiPolygonString, geometryCollectionString))
      types mustEqual Seq(POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION)
    }
    "merge up number types" in {
      val types = Seq[Any](1d, 1f, 1L, 1)
      foreach(types.drop(0).permutations.toSeq)(t => inferColType(t) mustEqual DOUBLE)
      foreach(types.drop(1).permutations.toSeq)(t => inferColType(t) mustEqual FLOAT)
      foreach(types.drop(2).permutations.toSeq)(t => inferColType(t) mustEqual LONG)
    }
    "merge up number types with lat/lon" in {
      inferColType(Seq(135, 45)) mustEqual INT
      inferColType(Seq(135f,45f)) mustEqual FLOAT
      inferColType(Seq(135d, 45d)) mustEqual DOUBLE
    }
    "merge up geometry types" in {
      val types = Seq(point, lineString, polygon, multiPoint, multiLineString, multiPolygon, geometryCollection)
      foreach(types.permutations.toSeq)(t => inferColType(t) mustEqual GEOMETRY)
    }
    "merge up null values" in {
      val values = Seq("a", 1, 1L, 1f, 1d, true, new Date(), Seq[Byte](0), uuid, point, lineString,
        polygon, multiPoint, multiLineString, multiPolygon, geometryCollection)
      foreach(values) { value =>
        inferColType(Seq(value, null)) mustEqual inferColType(Seq(value))
      }
    }
    "create points from lon/lat pairs" in {
      val floats = inferRowWithNames(Seq("lon" -> 45f, "lat" -> 55f, "foo" -> "foo"))
      floats.map(_.typed) mustEqual Seq(FLOAT, FLOAT, STRING, POINT)
      val doubles = inferRowWithNames(Seq("lon" -> 45d, "lat" -> 55d, "foo" -> "foo"))
      doubles.map(_.typed) mustEqual Seq(DOUBLE, DOUBLE, STRING, POINT)
      foreach(Seq(floats, doubles)) { types =>
        types(3).transform mustEqual DerivedTransform("point", types.head.name, types(1).name)
      }
    }
    "create points from lat/lon pairs" in {
      val floats = inferRowWithNames(Seq("lat" -> 45f, "lon" -> 120f, "foo" -> "foo"))
      floats.map(_.typed) mustEqual Seq(FLOAT, FLOAT, STRING, POINT)
      val doubles = inferRowWithNames(Seq("lat" -> 45d, "lon" -> 120d, "foo" -> "foo"))
      doubles.map(_.typed) mustEqual Seq(DOUBLE, DOUBLE, STRING, POINT)
      foreach(Seq(floats, doubles)) { types =>
        types(3).transform mustEqual DerivedTransform("point", types(1).name, types.head.name)
      }
    }
    "create points from named lat/lon fields" in {
      def infer(row: Seq[(String, Any)]): Seq[InferredType] =
        TypeInference.infer(row.map(v => PathWithValues(v._1, Seq(v._2))), Left("")).types.map(_.inferredType)

      val floats = infer(Seq("lat" -> 45f, "bar" -> 120f, "lon" -> 121f, "foo" -> "foo"))
      floats.map(_.typed) mustEqual Seq(FLOAT, FLOAT, FLOAT, STRING, POINT)
      val doubles = infer(Seq("lat" -> 45d, "bar" -> 120d, "lon" -> 121d, "foo" -> "foo"))
      doubles.map(_.typed) mustEqual Seq(DOUBLE, DOUBLE, DOUBLE, STRING, POINT)
      foreach(Seq(floats, doubles)) { types =>
        types(4).transform mustEqual DerivedTransform("point", types(2).name, types.head.name)
      }
    }
    "not create points from unpaired numbers" in {
      inferRowTypes(Seq(45f, "foo", 55f)) mustEqual Seq(FLOAT, STRING, FLOAT)
      inferRowTypes(Seq(45d, "foo", 55d)) mustEqual Seq(DOUBLE, STRING, DOUBLE)
    }
    "not create points if another geometry is present" in {
      inferRowTypes(Seq(45f, 55f, "POINT (40 50)")) mustEqual Seq(FLOAT, FLOAT, POINT)
      inferRowTypes(Seq(45d, 55d, "POINT (40 50)")) mustEqual Seq(DOUBLE, DOUBLE, POINT)
    }
    "not create points if values are not valid lat/lons" in {
      inferRowTypes(Seq(145f, 155f, "foo")) mustEqual Seq(FLOAT, FLOAT, STRING)
      inferRowTypes(Seq(145d, 155d, "foo")) mustEqual Seq(DOUBLE, DOUBLE, STRING)
    }
    "infer types despite some failures" in {
      val values = Seq.tabulate(11)(i => i) ++ Seq("foo")
      inferColType(values) mustEqual INT
      inferColType(values, Some(0.01f)) mustEqual STRING
    }
    "fall back to string type" in {
      inferColType(Seq("2018-01-01", uuidString)) mustEqual STRING
    }
  }
}
