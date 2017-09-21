/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import java.nio.charset.StandardCharsets

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.utils.GeoMessage.Change
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMessageSerializerTest extends Specification {

  private val sft = SimpleFeatureTypes.createType("KafkaGeoMessageTest", "name:String,*geom:Point:srid=4326")
  private val serializer = new GeoMessageSerializer(sft)
  private val feature = ScalaSimpleFeature.create(sft, "test_id", "foo", "POINT(1 -1)")

  "GeoMessageSerializer" should {
    "serialize a clear message" >> {
      val msg = GeoMessage.clear()

      val (key, value) = serializer.serialize(msg)

      key must not(beNull)
      value must not(beNull)
      key.length mustEqual 10
      key(0) mustEqual 1
      key(1) mustEqual 'X'
      value mustEqual Array.empty[Byte]

      serializer.deserialize(key, value) mustEqual msg
    }

    "serialize a delete message" >> {
      val msg = GeoMessage.delete(feature.getID)
      val (key, value) = serializer.serialize(msg)

      key must not(beNull)
      value must not(beNull)
      key.length mustEqual 10
      key(0) mustEqual 1
      key(1) mustEqual 'D'
      value mustEqual feature.getID.getBytes(StandardCharsets.UTF_8)

      serializer.deserialize(key, value) mustEqual msg
    }

    "serialize a change message" >> {
      val msg = GeoMessage.change(feature)
      val (key, value) = serializer.serialize(msg)

      key must not(beNull)
      value must not(beNull)
      key.length mustEqual 10
      key(0) mustEqual 1
      key(1) mustEqual 'C'

      val decoded = serializer.deserialize(key, value)
      decoded mustEqual msg
      decoded.asInstanceOf[Change].feature mustEqual feature
    }

    "throw an exception if the message key is null" >> {
      val key: Array[Byte] = null
      val msg = "garbage".getBytes(StandardCharsets.UTF_8)
      serializer.deserialize(key, msg) must throwAn[IllegalArgumentException]
    }

    "throw an exception if the message key length is incorrect" >> {
      val key = Array[Byte](1, 1, 1)
      val msg = "garbage".getBytes(StandardCharsets.UTF_8)
      serializer.deserialize(key, msg) must throwAn[IllegalArgumentException]
    }

    "throw an exception if the version number is incorrect" >> {
      val key = Array[Byte](0, 'X', 1, 2, 3, 4, 5, 6, 7, 8)
      val msg = "garbage".getBytes(StandardCharsets.UTF_8)
      serializer.deserialize(key, msg) must throwAn[IllegalArgumentException]
    }

    "throw an exception if the message type is invalid" >> {
      val key = Array[Byte](1, 'Z', 0, 0, 0, 0, 0, 0, 0, 0)
      val msg = "garbage".getBytes(StandardCharsets.UTF_8)
      serializer.deserialize(key, msg) must throwAn[IllegalArgumentException]
    }

    "throw an exception if the message cannot be decoded" >> {
      val key = Array[Byte](0, 'C', 1, 2, 3, 4, 5, 6, 7, 8)
      val msg = "garbage".getBytes(StandardCharsets.UTF_8)
      serializer.deserialize(key, msg) must throwAn[IllegalArgumentException]
    }
  }
}
