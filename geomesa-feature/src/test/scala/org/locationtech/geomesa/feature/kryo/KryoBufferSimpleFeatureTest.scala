/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.feature.kryo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.{Date, UUID}

import org.apache.commons.codec.binary.Base64
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.EncodingOption.EncodingOptions
import org.locationtech.geomesa.feature._
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.languageFeature.postfixOps

@RunWith(classOf[JUnitRunner])
class KryoBufferSimpleFeatureTest extends Specification {

  "KryoBufferSimpleFeature" should {

    "correctly deserialize basic features" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature("fakeid", sft)

      sf.setAttribute("a", "1")
      sf.setAttribute("b", "1.0")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("e", UUID.randomUUID())
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = new KryoFeatureCoder(sft, EncodingOptions.none)
      val serialized = serializer.encode(sf)

      val laz = serializer.lazyDecode(serialized)

      laz.getID mustEqual sf.getID
      laz.getAttribute("a") mustEqual sf.getAttribute("a")
      laz.getAttribute("b") mustEqual sf.getAttribute("b")
      laz.getAttribute("c") mustEqual sf.getAttribute("c")
      laz.getAttribute("d") mustEqual sf.getAttribute("d")
      laz.getAttribute("e") mustEqual sf.getAttribute("e")
      laz.getAttribute("f") mustEqual sf.getAttribute("f")
      laz.getAttribute("g") mustEqual sf.getAttribute("g")
      laz.getAttribute("dtg") mustEqual sf.getAttribute("dtg")
      laz.getAttribute("geom") mustEqual sf.getAttribute("geom")
    }

//    "correctly serialize and deserialize different geometries" in {
//      val spec = "a:LineString,b:Polygon,c:MultiPoint,d:MultiLineString,e:MultiPolygon," +
//        "f:GeometryCollection,dtg:Date,*geom:Point:srid=4326"
//      val sft = SimpleFeatureTypes.createType("testType", spec)
//      val sf = new ScalaSimpleFeature("fakeid", sft)
//
//      sf.setAttribute("a", "LINESTRING(0 2, 2 0, 8 6)")
//      sf.setAttribute("b", "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))")
//      sf.setAttribute("c", "MULTIPOINT(0 0, 2 2)")
//      sf.setAttribute("d", "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))")
//      sf.setAttribute("e", "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), " +
//        "((-1 5, 2 5, 2 2, -1 2, -1 5)))")
//      sf.setAttribute("f", "MULTIPOINT(0 0, 2 2)")
//      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
//      sf.setAttribute("geom", "POINT(55.0 49.0)")
//
//      "using byte arrays" >> {
//        val serializer = KryoFeatureSerializer(sft)
//
//        val serialized = serializer.write(sf)
//        val deserialized = serializer.read(serialized)
//
//        deserialized must not beNull;
//        deserialized.getType mustEqual sf.getType
//        deserialized.getAttributes mustEqual sf.getAttributes
//      }
//      "using streams" >> {
//        val serializer = KryoFeatureSerializer(sft)
//
//        val out = new ByteArrayOutputStream()
//        serializer.write(sf, out)
//        val in = new ByteArrayInputStream(out.toByteArray)
//        val deserialized = serializer.read(in)
//
//        deserialized must not beNull;
//        deserialized.getType mustEqual sf.getType
//        deserialized.getAttributes mustEqual sf.getAttributes
//      }
//    }

//    "correctly serialize and deserialize collection types" in {
//      val spec = "a:Integer,m:Map[String,Double],l:List[Date],dtg:Date,*geom:Point:srid=4326"
//      val sft = SimpleFeatureTypes.createType("testType", spec)
//      val sf = new ScalaSimpleFeature("fakeid", sft)
//
//      sf.setAttribute("a", "1")
//      sf.setAttribute("m", Map("test1" -> 1.0, "test2" -> 2.0))
//      sf.setAttribute("l", List(new Date(100), new Date(200)))
//      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
//      sf.setAttribute("geom", "POINT(45.0 49.0)")
//
//      "using byte arrays" >> {
//        val serializer = KryoFeatureSerializer(sft)
//
//        val serialized = serializer.write(sf)
//        val deserialized = serializer.read(serialized)
//
//        deserialized must not beNull;
//        deserialized.getType mustEqual sf.getType
//        deserialized.getAttributes mustEqual sf.getAttributes
//      }
//      "using streams" >> {
//        val serializer = KryoFeatureSerializer(sft)
//
//        val out = new ByteArrayOutputStream()
//        serializer.write(sf, out)
//        val in = new ByteArrayInputStream(out.toByteArray)
//        val deserialized = serializer.read(in)
//
//        deserialized must not beNull;
//        deserialized.getType mustEqual sf.getType
//        deserialized.getAttributes mustEqual sf.getAttributes
//      }
//    }

//    "correctly serialize and deserialize null values" in {
//      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,l:List,m:Map," +
//        "dtg:Date,*geom:Point:srid=4326"
//      val sft = SimpleFeatureTypes.createType("testType", spec)
//      val sf = new ScalaSimpleFeature("fakeid", sft)
//      "using byte arrays" >> {
//        val serializer = KryoFeatureSerializer(sft)
//
//        val serialized = serializer.write(sf)
//        val deserialized = serializer.read(serialized)
//
//        deserialized must not beNull;
//        deserialized.getType mustEqual sf.getType
//        deserialized.getAttributes.foreach(_ must beNull)
//        deserialized.getAttributes mustEqual sf.getAttributes
//      }
//      "using streams" >> {
//        val serializer = KryoFeatureSerializer(sft)
//
//        val out = new ByteArrayOutputStream()
//        serializer.write(sf, out)
//        val in = new ByteArrayInputStream(out.toByteArray)
//        val deserialized = serializer.read(in)
//
//        deserialized must not beNull;
//        deserialized.getType mustEqual sf.getType
//        deserialized.getAttributes.foreach(_ must beNull)
//        deserialized.getAttributes mustEqual sf.getAttributes
//      }
//    }

//    "correctly project features" in {
//
//      val sft = SimpleFeatureTypes.createType("fullType", "name:String,*geom:Point,dtg:Date")
//      val projectedSft = SimpleFeatureTypes.createType("projectedType", "*geom:Point")
//
//      val sf = new ScalaSimpleFeature("testFeature", sft)
//      sf.setAttribute("name", "foo")
//      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
//      sf.setAttribute("geom", "POINT(45.0 49.0)")
//
//      "when serializing" >> {
//        val serializer = KryoFeatureSerializer(sft, projectedSft, EncodingOptions.none)
//        val deserializer = KryoFeatureSerializer(projectedSft)
//
//        val serialized = serializer.write(sf)
//        val deserialized = deserializer.read(serialized)
//
//        deserialized.getID mustEqual sf.getID
//        deserialized.getDefaultGeometry mustEqual sf.getDefaultGeometry
//        deserialized.getAttributeCount mustEqual 1
//      }
//
//      "when deserializing" >> {
//        val serializer = KryoFeatureSerializer(sft)
//        val deserializer = KryoFeatureSerializer(sft, projectedSft, EncodingOptions.none)
//
//        val serialized = serializer.write(sf)
//        val deserialized = deserializer.read(serialized)
//
//        deserialized.getID mustEqual sf.getID
//        deserialized.getDefaultGeometry mustEqual sf.getDefaultGeometry
//        deserialized.getAttributeCount mustEqual 1
//      }
//    }

//    "be backwards compatible" in {
//      val spec = "dtg:Date,*geom:Point:srid=4326"
//      val sft = SimpleFeatureTypes.createType("testType", spec)
//
//      val sf = new ScalaSimpleFeature("fakeid", sft)
//      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
//      sf.setAttribute("geom", "POINT(45.0 49.0)")
//
//      val serializer = KryoFeatureSerializer(sft)
//      // base64 encoded bytes from version 0 of the kryo feature serializer
//      val version0SerializedBase64 = "AGZha2Vp5AEAAAE7+I60ABUAAAAAAUBGgAAAAAAAQEiAAAAAAAA="
//      val version0Bytes = Base64.decodeBase64(version0SerializedBase64)
//
//      val deserialized = serializer.read(version0Bytes)
//
//      deserialized must not beNull;
//      deserialized.getType mustEqual sf.getType
//      deserialized.getAttributes mustEqual sf.getAttributes
//    }

    "be faster than full deserialization" in {
//      skipped("integration")
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("speed", spec)

      val sf = new ScalaSimpleFeature("fakeid", sft)

      sf.setAttribute("a", "1")
      sf.setAttribute("b", "1.0")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("e", UUID.randomUUID())
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = new KryoFeatureCoder(sft, EncodingOptions.none)
      val serialized = serializer.encode(sf)

//      val start = System.currentTimeMillis()
//      (0 until 1000000).foreach { _ =>
//        val de = serializer.read(serialized)
//        de.getAttribute(1)
//      }
//      println(s"took ${System.currentTimeMillis() - start}ms")

      val start2 = System.currentTimeMillis()
      var reusable = serializer.lazyDecode(serialized, null)
      (0 until 1000000).foreach { _ =>
        val laz = serializer.lazyDecode(serialized, reusable)
        laz.getAttribute(7)
      }
      println(s"took ${System.currentTimeMillis() - start2}ms")

      println
      println
      success
    }
  }
}
