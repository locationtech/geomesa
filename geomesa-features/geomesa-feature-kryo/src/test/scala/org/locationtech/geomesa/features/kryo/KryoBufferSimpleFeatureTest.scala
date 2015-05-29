/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.features.kryo

import java.util.{Date, UUID}

import org.junit.runner.RunWith

import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.collection.JavaConversions._

//import scala.languageFeature.postfixOps

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

      val serializer = new KryoFeatureSerializer(sft, SerializationOptions.none)
      val serialized = serializer.serialize(sf)

      val laz = serializer.getReusableFeature
      laz.setBuffer(serialized)

      laz.getID mustEqual sf.getID
      laz.getAttributes mustEqual sf.getAttributes
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

      val serializer = new KryoFeatureSerializer(sft)

      val serialized = serializer.write(sf)
      val laz = serializer.getReusableFeature
      laz.setBuffer(serialized)

      laz must not(beNull)
      laz.getType mustEqual sf.getType
      laz.getAttributes mustEqual sf.getAttributes
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

      val serializer = new KryoFeatureSerializer(sft)

      val serialized = serializer.write(sf)
      val laz = serializer.getReusableFeature
      laz.setBuffer(serialized)

      laz must not(beNull)
      laz.getType mustEqual sf.getType
      laz.getAttributes mustEqual sf.getAttributes
    }

    "correctly serialize and deserialize null values" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,l:List,m:Map," +
        "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature("fakeid", sft)

      val serializer = new KryoFeatureSerializer(sft)

      val serialized = serializer.write(sf)
      val laz = serializer.getReusableFeature
      laz.setBuffer(serialized)

      laz must not(beNull)
      laz.getType mustEqual sf.getType
      forall(laz.getAttributes)((o: AnyRef) => o must beNull)
      laz.getAttributes mustEqual sf.getAttributes
    }

    "correctly project features" in {
      val sft = SimpleFeatureTypes.createType("fullType", "name:String,*geom:Point,dtg:Date")
      val projectedSft = SimpleFeatureTypes.createType("projectedType", "*geom:Point")

      val sf = new ScalaSimpleFeature("testFeature", sft)
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = new KryoFeatureSerializer(sft)
      val serialized = serializer.serialize(sf)

      val laz = serializer.getReusableFeature
      laz.setBuffer(serialized)
      laz.setTransforms("geom:geom", projectedSft)

      val transformed = new KryoFeatureSerializer(projectedSft).deserialize(laz.transform())

      transformed.getID mustEqual sf.getID
      transformed.getDefaultGeometry mustEqual sf.getDefaultGeometry
      transformed.getAttributeCount mustEqual 1
    }

    "be faster than full deserialization" in {
      skipped("integration")
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

      val serializer = new KryoFeatureSerializer(sft, SerializationOptions.none)
      val serialized = serializer.serialize(sf)

      val start = System.currentTimeMillis()
      (0 until 1000000).foreach { _ =>
        val de = serializer.read(serialized)
        de.getAttribute(1)
      }
      println(s"took ${System.currentTimeMillis() - start}ms")

      val start2 = System.currentTimeMillis()
      val reusable = serializer.getReusableFeature
      (0 until 1000000).foreach { _ =>
        reusable.setBuffer(serialized)
        reusable.getAttribute(7)
      }
      println(s"took ${System.currentTimeMillis() - start2}ms")

      println()
      println()
      success
    }
  }
}
