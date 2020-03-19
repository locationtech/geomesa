/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import java.io.StringWriter
import java.util.{Date, UUID}

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoJsonSerializerTest extends Specification {

  "GeoJsonSerializer" should {

    "serialize single feature" in {
      val spec = "age:Integer,name:String,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("age", "1")
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = new GeoJsonSerializer(sft)

      val out = new StringWriter()
      WithClose(GeoJsonSerializer.writer(out))(serializer.write(_, sf))

      val serialized = out.toString

      serialized mustEqual
          """{
            |"type":"Feature",
            |"id":"fakeid",
            |"geometry":{"type":"Point","coordinates":[45,49]},
            |"properties":{
            |"age":1,
            |"name":"foo",
            |"dtg":"2013-01-02T00:00:00.000Z"}
            |}""".stripMargin.replaceAll("\n", "")
    }

    "serialize multiple features" in {
      val spec = "age:Integer,name:String,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("age", "1")
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = new GeoJsonSerializer(sft)

      val out = new StringWriter()
      WithClose(GeoJsonSerializer.writer(out)) { writer =>
        serializer.write(writer, sf)
        serializer.write(writer, sf)
      }

      val serialized = out.toString

      val expected =
        """{
          |"type":"Feature",
          |"id":"fakeid",
          |"geometry":{"type":"Point","coordinates":[45,49]},
          |"properties":{
          |"age":1,
          |"name":"foo",
          |"dtg":"2013-01-02T00:00:00.000Z"}
          |}""".stripMargin.replaceAll("\n", "")

      serialized mustEqual expected + expected
    }

    "serialize feature collections" in {
      val spec = "age:Integer,name:String,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("age", "1")
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = new GeoJsonSerializer(sft)

      val out = new StringWriter()
      WithClose(GeoJsonSerializer.writer(out)) { writer =>
        serializer.startFeatureCollection(writer)
        serializer.write(writer, sf)
        serializer.write(writer, sf)
        serializer.endFeatureCollection(writer)
      }

      val serialized = out.toString

      val expected =
        """{
          |"type":"Feature",
          |"id":"fakeid",
          |"geometry":{"type":"Point","coordinates":[45,49]},
          |"properties":{
          |"age":1,
          |"name":"foo",
          |"dtg":"2013-01-02T00:00:00.000Z"}
          |}""".stripMargin.replaceAll("\n", "")

      serialized mustEqual """{"type":"FeatureCollection","features":[""" + expected + "," + expected + "]}"
    }

    "serialize basic attribute types" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "1")
      sf.setAttribute("b", "1.0")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("e", UUID.fromString("48c0b012-a80e-40fb-9a72-14cdcbc88585"))
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = new GeoJsonSerializer(sft)

      val out = new StringWriter()
      WithClose(GeoJsonSerializer.writer(out))(serializer.write(_, sf))

      val serialized = out.toString

      serialized mustEqual
          """{
            |"type":"Feature",
            |"id":"fakeid",
            |"geometry":{"type":"Point","coordinates":[45,49]},
            |"properties":{
            |"a":1,
            |"b":1.0,
            |"c":5.37,
            |"d":-100,
            |"e":"48c0b012-a80e-40fb-9a72-14cdcbc88585",
            |"f":"mystring",
            |"g":false,
            |"dtg":"2013-01-02T00:00:00.000Z"}
            |}""".stripMargin.replaceAll("\n", "")
    }

    "serialize different geometries" in {
      val spec = "a:LineString,b:Polygon,c:MultiPoint,d:MultiLineString,e:MultiPolygon," +
        "f:GeometryCollection,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "LINESTRING(0 2, 2 0, 8 6)")
      sf.setAttribute("b", "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))")
      sf.setAttribute("c", "MULTIPOINT(0 0, 2 2)")
      sf.setAttribute("d", "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))")
      sf.setAttribute("e", "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), " +
        "((-1 5, 2 5, 2 2, -1 2, -1 5)))")
      sf.setAttribute("f", "MULTIPOINT(0 0, 2 2)")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(55.0 49.0)")

      val serializer = new GeoJsonSerializer(sft)

      val out = new StringWriter()
      WithClose(GeoJsonSerializer.writer(out))(serializer.write(_, sf))

      val serialized = out.toString

      serialized mustEqual
          """
            |{
            |"type":"Feature",
            |"id":"fakeid",
            |"geometry":{"type":"Point","coordinates":[55,49]},
            |"properties":{
            |"a":{"type":"LineString","coordinates":[[0.0,2],[2,0.0],[8,6]]},
            |"b":{"type":"Polygon","coordinates":[[[20,10],[30,0.0],[40,10],[30,20],[20,10]]]},
            |"c":{"type":"MultiPoint","coordinates":[[0.0,0.0],[2,2]]},
            |"d":{"type":"MultiLineString","coordinates":[[[0.0,2],[2,0.0],[8,6]],[[0.0,2],[2,0.0],[8,6]]]},
            |"e":{"type":"MultiPolygon","coordinates":[[[[-1,0.0],[0.0,1],[1,0.0],[0.0,-1],[-1,0.0]]],[[[-2,6],[1,6],[1,3],[-2,3],[-2,6]]],[[[-1,5],[2,5],[2,2],[-1,2],[-1,5]]]]},
            |"f":{"type":"MultiPoint","coordinates":[[0.0,0.0],[2,2]]},"dtg":"2013-01-02T00:00:00.000Z"}
            |}""".stripMargin.replaceAll("\n", "")
    }

    "serialize collection types" in {
      val spec = "m:Map[String,Double],l:List[Date],*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("m", Map("test1" -> 1.0, "test2" -> 2.0))
      sf.setAttribute("l", List(new Date(100), new Date(200)))
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = new GeoJsonSerializer(sft)

      val out = new StringWriter()
      WithClose(GeoJsonSerializer.writer(out))(serializer.write(_, sf))

      val serialized = out.toString

      serialized mustEqual
          """{
            |"type":"Feature",
            |"id":"fakeid",
            |"geometry":{"type":"Point","coordinates":[45,49]},
            |"properties":{
            |"m":{"test1":1.0,"test2":2.0},
            |"l":["1970-01-01T00:00:00.100Z","1970-01-01T00:00:00.200Z"]}
            |}""".stripMargin.replaceAll("\n", "")
    }

    "serialize null values" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,l:List,m:Map," +
        "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      val serializer = new GeoJsonSerializer(sft)

      val out = new StringWriter()
      WithClose(GeoJsonSerializer.writer(out))(serializer.write(_, sf))

      val serialized = out.toString

      serialized mustEqual
          """
            |{
            |"type":"Feature",
            |"id":"fakeid",
            |"geometry":null,
            |"properties":{
            |"a":null,
            |"b":null,
            |"c":null,
            |"d":null,
            |"e":null,
            |"f":null,
            |"g":null,
            |"l":null,
            |"m":null,
            |"dtg":null}
            |}""".stripMargin.replaceAll("\n", "")
    }
  }
}
