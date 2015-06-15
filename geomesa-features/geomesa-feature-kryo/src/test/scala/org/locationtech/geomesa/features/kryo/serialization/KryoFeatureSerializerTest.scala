/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.kryo.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.{Date, UUID}

import org.apache.commons.codec.binary.Base64
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.languageFeature.postfixOps

@RunWith(classOf[JUnitRunner])
class KryoFeatureSerializerTest extends Specification {

  "KryoFeatureSerializer" should {

    "correctly serialize and deserialize basic features" in {
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

      "using byte arrays" >> {
        val serializer = KryoFeatureSerializer(sft)

        val serialized = serializer.write(sf)
        val deserialized = serializer.read(serialized)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
      }
      "using streams" >> {
        val serializer = KryoFeatureSerializer(sft)

        val out = new ByteArrayOutputStream()
        serializer.write(sf, out)
        val in = new ByteArrayInputStream(out.toByteArray)
        val deserialized = serializer.read(in)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
      }

      "without user data" >> {
        val sft = SimpleFeatureTypes.createType("testType", "dtg:Date,*geom:Point:srid=4326")
        val sf = new ScalaSimpleFeature("testId", sft)

        sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
        sf.setAttribute("geom", "POINT(45.0 49.0)")

        val vis = "u&usa&fouo"
        SecurityUtils.setFeatureVisibility(sf, vis)

        val serializer = KryoFeatureSerializer(sft, SerializationOptions.none)

        val serialized = serializer.write(sf)
        val deserialized = serializer.read(serialized)

        SecurityUtils.getVisibility(deserialized) must beNull
      }

      "with user data" >> {
        val sft = SimpleFeatureTypes.createType("testType", "dtg:Date,*geom:Point:srid=4326")
        val sf = new ScalaSimpleFeature("testId", sft)

        sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
        sf.setAttribute("geom", "POINT(45.0 49.0)")

        val vis = "u&usa&fouo"
        SecurityUtils.setFeatureVisibility(sf, vis)

        val serializer = KryoFeatureSerializer(sft, SerializationOptions.withUserData)

        val serialized = serializer.write(sf)
        val deserialized = serializer.read(serialized)

        deserialized.getUserData mustEqual sf.getUserData
      }
    }

    "correctly serialize and deserialize different geometries" in {
      val spec = "a:LineString,b:Polygon,c:MultiPoint,d:MultiLineString,e:MultiPolygon," +
        "f:GeometryCollection,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature("fakeid", sft)

      sf.setAttribute("a", "LINESTRING(0 2, 2 0, 8 6)")
      sf.setAttribute("b", "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))")
      sf.setAttribute("c", "MULTIPOINT(0 0, 2 2)")
      sf.setAttribute("d", "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))")
      sf.setAttribute("e", "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), " +
        "((-1 5, 2 5, 2 2, -1 2, -1 5)))")
      sf.setAttribute("f", "MULTIPOINT(0 0, 2 2)")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(55.0 49.0)")

      "using byte arrays" >> {
        val serializer = KryoFeatureSerializer(sft)

        val serialized = serializer.write(sf)
        val deserialized = serializer.read(serialized)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
      }
      "using streams" >> {
        val serializer = KryoFeatureSerializer(sft)

        val out = new ByteArrayOutputStream()
        serializer.write(sf, out)
        val in = new ByteArrayInputStream(out.toByteArray)
        val deserialized = serializer.read(in)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
      }
    }

    "correctly serialize and deserialize collection types" in {
      val spec = "a:Integer,m:Map[String,Double],l:List[Date],dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature("fakeid", sft)

      sf.setAttribute("a", "1")
      sf.setAttribute("m", Map("test1" -> 1.0, "test2" -> 2.0))
      sf.setAttribute("l", List(new Date(100), new Date(200)))
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      "using byte arrays" >> {
        val serializer = KryoFeatureSerializer(sft)

        val serialized = serializer.write(sf)
        val deserialized = serializer.read(serialized)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
      }
      "using streams" >> {
        val serializer = KryoFeatureSerializer(sft)

        val out = new ByteArrayOutputStream()
        serializer.write(sf, out)
        val in = new ByteArrayInputStream(out.toByteArray)
        val deserialized = serializer.read(in)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
      }
    }

    // NB: this doesn't actually seem to cause the error I was seeing, but
    // ScaldingDelimitedIngestJobTest in geomesa-tools does cause it...
    "correctly serialize with a type having the same field names but different field types" in {
      val spec = "a:Integer,m:List[String],l:Map[Double,Date],dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature("fakeid", sft)

      sf.setAttribute("a", "1")
      sf.setAttribute("m", List("test1", "test2"))
      sf.setAttribute("l", Map(1.0 -> new Date(100), 2.0 -> new Date(200)))
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      "using byte arrays" >> {
        val serializer = KryoFeatureSerializer(sft)

        val serialized = serializer.write(sf)
        val deserialized = serializer.read(serialized)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
      }
      "using streams" >> {
        val serializer = KryoFeatureSerializer(sft)

        val out = new ByteArrayOutputStream()
        serializer.write(sf, out)
        val in = new ByteArrayInputStream(out.toByteArray)
        val deserialized = serializer.read(in)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
      }
    }


    "correctly serialize and deserialize null values" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,l:List,m:Map," +
        "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature("fakeid", sft)
      "using byte arrays" >> {
        val serializer = KryoFeatureSerializer(sft)

        val serialized = serializer.write(sf)
        val deserialized = serializer.read(serialized)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes.foreach(_ must beNull)
        deserialized.getAttributes mustEqual sf.getAttributes
      }
      "using streams" >> {
        val serializer = KryoFeatureSerializer(sft)

        val out = new ByteArrayOutputStream()
        serializer.write(sf, out)
        val in = new ByteArrayInputStream(out.toByteArray)
        val deserialized = serializer.read(in)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes.foreach(_ must beNull)
        deserialized.getAttributes mustEqual sf.getAttributes
      }
    }

    "correctly project features" in {

      val sft = SimpleFeatureTypes.createType("fullType", "name:String,*geom:Point,dtg:Date")
      val projectedSft = SimpleFeatureTypes.createType("projectedType", "*geom:Point")

      val sf = new ScalaSimpleFeature("testFeature", sft)
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      "when serializing" >> {
        val serializer = KryoFeatureSerializer(sft, projectedSft, SerializationOptions.none)
        val deserializer = KryoFeatureSerializer(projectedSft)

        val serialized = serializer.write(sf)
        val deserialized = deserializer.read(serialized)

        deserialized.getID mustEqual sf.getID
        deserialized.getDefaultGeometry mustEqual sf.getDefaultGeometry
        deserialized.getAttributeCount mustEqual 1
      }

      "when deserializing" >> {
        val serializer = KryoFeatureSerializer(sft)
        val deserializer = KryoFeatureSerializer(sft, projectedSft, SerializationOptions.none)

        val serialized = serializer.write(sf)
        val deserialized = deserializer.read(serialized)

        deserialized.getID mustEqual sf.getID
        deserialized.getDefaultGeometry mustEqual sf.getDefaultGeometry
        deserialized.getAttributeCount mustEqual 1
      }
    }

    "be backwards compatible" in {
      val spec = "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)

      val sf = new ScalaSimpleFeature("fakeid", sft)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = KryoFeatureSerializer(sft)
      // base64 encoded bytes from version 0 of the kryo feature serializer
      val version0SerializedBase64 = "AGZha2Vp5AEAAAE7+I60ABUAAAAAAUBGgAAAAAAAQEiAAAAAAAA="
      val version0Bytes = Base64.decodeBase64(version0SerializedBase64)

      val deserialized = serializer.read(version0Bytes)

      deserialized must not beNull;
      deserialized.getType mustEqual sf.getType
      deserialized.getAttributes mustEqual sf.getAttributes
    }

    "be faster than old version" in {
      skipped("integration")
      val spec = "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)

      val sf = new ScalaSimpleFeature("fakeid", sft)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer0 = KryoFeatureSerializer(sft)
      val serializer1 = KryoFeatureSerializer(sft)

      val version0SerializedBase64 = "AGZha2Vp5AEAAAE7+I60ABUAAAAAAUBGgAAAAAAAQEiAAAAAAAA="
      val version0Bytes = Base64.decodeBase64(version0SerializedBase64)

      val version1Bytes = serializer1.write(sf)

      // prime the serialization
      serializer0.read(version0Bytes)
      serializer1.read(version1Bytes)

      val start2 = System.currentTimeMillis()
      (0 until 1000000).foreach { _ =>
        serializer0.read(version0Bytes)
      }
      println(s"took ${System.currentTimeMillis() - start2}ms")

      val start = System.currentTimeMillis()
      (0 until 1000000).foreach { _ =>
        serializer1.read(version1Bytes)
      }
      println(s"took ${System.currentTimeMillis() - start}ms")

      val start3 = System.currentTimeMillis()
      (0 until 1000000).foreach { _ =>
        serializer1.read(version1Bytes)
      }
      println(s"took ${System.currentTimeMillis() - start3}ms")

      val start4 = System.currentTimeMillis()
      (0 until 1000000).foreach { _ =>
        serializer0.read(version0Bytes)
      }
      println(s"took ${System.currentTimeMillis() - start4}ms")

      println
      println
      success
    }
  }
}
