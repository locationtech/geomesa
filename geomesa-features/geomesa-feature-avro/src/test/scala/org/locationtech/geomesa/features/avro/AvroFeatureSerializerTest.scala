/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import java.nio.charset.StandardCharsets
import java.util
import java.util.{Date, UUID}

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.AbstractSimpleFeature.AbstractImmutableSimpleFeature
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SerializationOption}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.geotools.{ImmutableFeatureId, SimpleFeatureTypes}
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AvroFeatureSerializerTest extends Specification with LazyLogging {

  import SerializationOption._

  val options = Seq(
      Set.empty[SerializationOption],
//      Set(Immutable),
      Set(WithUserData)
//      Set(Lazy),
//      Set(Immutable, WithUserData),
//      Set(Lazy, Immutable),
//      Set(Lazy, WithUserData),
//      Set(Lazy, Immutable, WithUserData)
    )

  "new AvroFeatureSerializer" should {

    def arrayEquals(a: Any, b: Any): MatchResult[Boolean] = {
      val aBytes = a.asInstanceOf[Array[Byte]]
      val bBytes = b.asInstanceOf[Array[Byte]]
      util.Arrays.equals(aBytes, bBytes) must beTrue
    }

    "correctly deserialize basic features" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326,bytes:Bytes"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      val userData = Map("key.one" -> java.lang.Boolean.TRUE, "key.two" -> "value.two")

      sf.setAttribute("a", "1")
      sf.setAttribute("b", "1.0")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("e", UUID.randomUUID())
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")
      sf.setAttribute("bytes", "\u0000FOOBARBAZ\u0000\u4444123".getBytes(StandardCharsets.UTF_16BE))
      sf.getUserData.putAll(userData)

      forall(options) { opts =>
        val serializer = new AvroFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val deserialized = serializer.deserialize(serialized)

        deserialized.getID mustEqual sf.getID
        deserialized.getAttributes.dropRight(1) mustEqual sf.getAttributes.dropRight(1)
        arrayEquals(sf.getAttributes.last, "\u0000FOOBARBAZ\u0000\u4444123".getBytes(StandardCharsets.UTF_16BE))
        arrayEquals(deserialized.getAttributes.last, sf.getAttributes.last)

        if (opts.withUserData) {
          deserialized.getUserData.toMap mustEqual userData
        } else {
          deserialized.getUserData must beEmpty
        }

        if (opts.immutable) {
          deserialized must beAnInstanceOf[AbstractImmutableSimpleFeature]
          deserialized.getIdentifier must beAnInstanceOf[ImmutableFeatureId]
          deserialized.setAttribute(0, 2) must throwAn[UnsupportedOperationException]
          deserialized.setAttribute("a", 2) must throwAn[UnsupportedOperationException]
          deserialized.setAttributes(Array.empty[AnyRef]) must throwAn[UnsupportedOperationException]
          deserialized.setAttributes(Seq.empty[AnyRef]) must throwAn[UnsupportedOperationException]
          deserialized.getUserData.put("foo", "bar") must throwAn[UnsupportedOperationException]
        } else {
          deserialized.getUserData.put("foo", "bar")
          deserialized.getUserData.get("foo") mustEqual "bar"
        }
      }
    }

    "correctly serialize and deserialize different geometries" in {
      val spec = "a:LineString,b:Polygon,c:MultiPoint,d:MultiLineString,e:MultiPolygon," +
        "f:GeometryCollection,dtg:Date,*geom:Point:srid=4326"
      val sftWkb = SimpleFeatureTypes.createType("testTypeWkb", spec)
      // use a different name to avoid cached serializers
      val sftTwkb = SimpleFeatureTypes.createType("testTypeTwkb", spec)
      sftTwkb.getAttributeDescriptors.foreach(_.getUserData.put(AttributeOptions.OPT_PRECISION, "6"))

      val sf = new ScalaSimpleFeature(sftWkb, "fakeid")
      sf.setAttribute("a", "LINESTRING(0 2, 2 0, 8 6)")
      sf.setAttribute("b", "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))")
      sf.setAttribute("c", "MULTIPOINT(0 0, 2 2)")
      sf.setAttribute("d", "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))")
      sf.setAttribute("e", "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), " +
          "((-1 5, 2 5, 2 2, -1 2, -1 5)))")
      sf.setAttribute("f", "MULTIPOINT(0 0, 2 2)")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(55.0 49.0)")

      forall(Seq(sftWkb, sftTwkb)) { sft =>
        forall(options) { opts =>
          val serializer = new AvroFeatureSerializer(sft, opts)
          val serialized = serializer.serialize(sf)
          val deserialized = serializer.deserialize(serialized)

          deserialized must not(beNull)
          deserialized.getType mustEqual sft
          deserialized.getAttributes mustEqual sf.getAttributes
        }
      }
    }

    "correctly serialize and deserialize geometries with n dimensions" in {
      val spec = "a:LineString,b:Polygon,c:MultiPoint,d:MultiLineString,e:MultiPolygon," +
          "f:GeometryCollection,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "LINESTRING(0 2 0, 2 0 1, 8 6 2)")
      sf.setAttribute("b", "POLYGON((20 10 0, 30 0 10, 40 10 10, 30 20 0, 20 10 0))")
      sf.setAttribute("c", "MULTIPOINT(0 0 0, 2 2 2)")
      sf.setAttribute("d", "MULTILINESTRING((0 2 0, 2 0 1, 8 6 2),(0 2 0, 2 0 0, 8 6 0))")
      sf.setAttribute("e", "MULTIPOLYGON(((-1 0 0, 0 1 0, 1 0 0, 0 -1 0, -1 0 0)), ((-2 6 2, 1 6 3, 1 3 3, -2 3 3, -2 6 2)), " +
          "((-1 5 0, 2 5 0, 2 2 0, -1 2 0, -1 5 0)))")
      sf.setAttribute("f", "MULTIPOINT(0 0 2, 2 2 0)")
      sf.setAttribute("geom", "POINT(55.0 49.0 37.0)")

      forall(options) { opts =>
        val serializer = new AvroFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val deserialized = serializer.deserialize(serialized)

        deserialized must not(beNull)
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
        forall(deserialized.getAttributes.zip(sf.getAttributes)) { case (left, right) =>
          forall(left.asInstanceOf[Geometry].getCoordinates.zip(right.asInstanceOf[Geometry].getCoordinates)) {
            case (c1, c2) => c1.equals3D(c2) must beTrue
          }
        }
      }
    }.pendingUntilFixed()

    "correctly serialize and deserialize collection types" in {
      val spec = "a:Integer,m:Map[String,Double],l:List[Date],dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "1")
      sf.setAttribute("m", Map("test1" -> 1.0, "test2" -> 2.0))
      sf.setAttribute("l", List(new Date(100), new Date(200)))
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      forall(options) { opts =>
        val serializer = new AvroFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val deserialized = serializer.deserialize(serialized)

        deserialized must not(beNull)
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
      }
    }

    "serialize maps and lists of bytes" >> {
      val spec = "m1:Map[String,Bytes],l:List[Bytes],dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("m1", Map("a" -> Array(0.toByte, 23.toByte)))
      sf.setAttribute("l", List[Array[Byte]](Array(0.toByte, 23.toByte), Array(1.toByte)))
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      forall(options) { opts =>
        val serializer = new AvroFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val deserialized = serializer.deserialize(serialized)

        deserialized must not(beNull)
        deserialized.getType mustEqual sf.getType
        import org.locationtech.geomesa.utils.geotools.Conversions._
        arrayEquals(deserialized.get[java.util.Map[String,_]]("m1")("a"), sf.get[java.util.Map[String,_]]("m1")("a"))
        arrayEquals(deserialized.get[java.util.List[_]]("l")(0), sf.get[java.util.List[_]]("l")(0))
      }
    }

    "correctly serialize and deserialize null values" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,l:List,m:Map," +
        "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      forall(options) { opts =>
        val serializer = new AvroFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val deserialized = serializer.deserialize(serialized)

        deserialized must not(beNull)
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes.foreach(_ must beNull)
        deserialized.getAttributes mustEqual sf.getAttributes
      }
    }

    "correctly serialize and deserialize sub-arrays" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      val userData = Map("key.one" -> java.lang.Boolean.TRUE, "key.two" -> "value.two")

      sf.setAttribute("a", "1")
      sf.setAttribute("b", "1.0")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("e", UUID.randomUUID())
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")
      sf.getUserData.putAll(userData)

      forall(options) { opts =>
        val serializer = new AvroFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val extra = Array.fill[Byte](128)(-1)
        val bytes = Seq((serialized ++ extra, 0), (extra ++ serialized, extra.length), (extra ++ serialized ++ extra, extra.length))

        forall(bytes) { case (array, offset) =>
          val deserialized = serializer.deserialize(array, offset, serialized.length)
          deserialized must not(beNull)
          deserialized.getType mustEqual sf.getType
          deserialized.getAttributes mustEqual sf.getAttributes

          if (opts.withUserData) {
            deserialized.getUserData.toMap mustEqual userData
          } else {
            deserialized.getUserData must beEmpty
          }

          if (opts.immutable) {
            deserialized must beAnInstanceOf[AbstractImmutableSimpleFeature]
            deserialized.getIdentifier must beAnInstanceOf[ImmutableFeatureId]
            deserialized.setAttribute(0, 2) must throwAn[UnsupportedOperationException]
            deserialized.setAttribute("a", 2) must throwAn[UnsupportedOperationException]
            deserialized.setAttributes(Array.empty[AnyRef]) must throwAn[UnsupportedOperationException]
            deserialized.setAttributes(Seq.empty[AnyRef]) must throwAn[UnsupportedOperationException]
            deserialized.getUserData.put("foo", "bar") must throwAn[UnsupportedOperationException]
          } else {
            deserialized.getUserData.put("foo", "bar")
            deserialized.getUserData.get("foo") mustEqual "bar"
          }
        }
      }
    }

    "correctly project features" in {
      val sft = SimpleFeatureTypes.createType("fullType", "name:String,*geom:Point,dtg:Date")
      val projectedSft = SimpleFeatureTypes.createType("projectedType", "*geom:Point")

      val sf = new ScalaSimpleFeature(sft, "testFeature")
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = new AvroFeatureSerializer(sft)
      val deserializer = new ProjectingAvroFeatureDeserializer(sft, projectedSft)

      val serialized = serializer.serialize(sf)
      val deserialized = deserializer.deserialize(serialized)

      deserialized.getID mustEqual sf.getID
      deserialized.getDefaultGeometry mustEqual sf.getDefaultGeometry
      deserialized.getAttributeCount mustEqual 1
    }

    "correctly project features to larger sfts" in {
      val sft = SimpleFeatureTypes.createType("fullType", "name:String,*geom:Point,dtg:Date")
      val projectedSft = SimpleFeatureTypes.createType("projectedType",
        "name1:String,name2:String,*geom:Point,otherDate:Date")

      val sf = new ScalaSimpleFeature(sft, "testFeature")
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = new AvroFeatureSerializer(sft)
      val deserializer = new ProjectingAvroFeatureDeserializer(sft, projectedSft)

      val serialized = serializer.serialize(sf)
      val deserialized = deserializer.deserialize(serialized)

      deserialized.getID mustEqual sf.getID
      deserialized.getDefaultGeometry mustEqual sf.getDefaultGeometry
      deserialized.getAttributeCount mustEqual 4
    }
  }
}
