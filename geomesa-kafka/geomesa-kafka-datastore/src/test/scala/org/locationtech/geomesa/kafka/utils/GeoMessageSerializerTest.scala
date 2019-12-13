/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import java.nio.charset.StandardCharsets
import java.util.Base64

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SerializationType}
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Change, Clear, Delete}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMessageSerializerTest extends Specification {

  private val sft = SimpleFeatureTypes.createType("KafkaGeoMessageTest", "name:String,*geom:Point:srid=4326")
  private val serializers = SerializationType.values.toSeq.map(GeoMessageSerializer(sft, _))
  private val feature = ScalaSimpleFeature.create(sft, "test_id", "foo", "POINT(1 -1)")

  "GeoMessageSerializer" should {
    "serialize a clear message" >> {
      val msg = GeoMessage.clear()

      foreach(serializers) { serializer =>
        val (key, value, headers) = serializer.serialize(msg)

        key must not(beNull)
        value must not(beNull)
        headers must not(beNull)

        foreach(serializers) { deserializer =>
          deserializer.deserialize(key, value, headers) mustEqual msg
          // check compatibility with older kafka versions that don't support headers
          deserializer.deserialize(key, value) mustEqual msg
        }
      }
    }

    "serialize a delete message" >> {
      val msg = GeoMessage.delete(feature.getID)

      foreach(serializers) { serializer =>
        val (key, value, headers) = serializer.serialize(msg)

        key must not(beNull)
        value must beNull
        headers must not(beNull)

        foreach(serializers) { deserializer =>
          deserializer.deserialize(key, value, headers) mustEqual msg
          // check compatibility with older kafka versions that don't support headers
          deserializer.deserialize(key, value) mustEqual msg
        }
      }
    }

    "serialize a change message" >> {
      val msg = GeoMessage.change(feature)

      forall(serializers) { serializer =>
        val (key, value, headers) = serializer.serialize(msg)

        key must not(beNull)
        value must not(beNull)
        headers must not(beNull)

        forall(serializers) { deserializer =>
          deserializer.deserialize(key, value, headers) mustEqual msg
          // check compatibility with older kafka versions that don't support headers
          serializer.deserialize(key, value) mustEqual msg
        }
      }
    }

    "deserialize version one clear messages" >> {
      val decoder = Base64.getDecoder
      val key = decoder.decode("AVgAAAFkbDTpaw==")
      val value = Array.empty[Byte]

      foreach(serializers) { serializer =>
        val decoded = serializer.deserialize(key, value)
        decoded must beAnInstanceOf[Clear]
      }
    }

    "deserialize version one delete messages" >> {
      val decoder = Base64.getDecoder
      val key = decoder.decode("AUQAAAFkbDTpag==")
      val value = decoder.decode("dGVzdF9pZA==")

      foreach(serializers) { serializer =>
        val decoded = serializer.deserialize(key, value)
        decoded must beAnInstanceOf[Delete]
        decoded.asInstanceOf[Delete].id mustEqual feature.getID
      }
    }

    "deserialize version one change messages" >> {
      val decoder = Base64.getDecoder
      val key = decoder.decode("AUMAAAFkbDTpZg==")
      val value = decoder.decode("AgAAACp0ZXN0X2nkZm/vAQgDP/AAAAAAAAC/8AAAAAAAAH/4AAAAAAAADA8AAAAA")

      foreach(serializers) { serializer =>
        val decoded = serializer.deserialize(key, value)
        decoded must beAnInstanceOf[Change]
        decoded.asInstanceOf[Change].feature mustEqual feature
      }
    }

    "throw an exception if the message key is null" >> {
      val key: Array[Byte] = null
      val msg = "garbage".getBytes(StandardCharsets.UTF_8)
      foreach(serializers) { serializer =>
        serializer.deserialize(key, msg) must throwAn[IllegalArgumentException]
      }
    }

    "throw an exception if the message key length is incorrect" >> {
      val key = Array[Byte](1, 1, 1)
      val msg = "garbage".getBytes(StandardCharsets.UTF_8)
      foreach(serializers) { serializer =>
        serializer.deserialize(key, msg) must throwAn[IllegalArgumentException]
      }
    }

    "throw an exception if the version number is incorrect" >> {
      val key = Array[Byte](0, 'X', 1, 2, 3, 4, 5, 6, 7, 8)
      val msg = "garbage".getBytes(StandardCharsets.UTF_8)
      foreach(serializers) { serializer =>
        serializer.deserialize(key, msg) must throwAn[IllegalArgumentException]
      }
    }

    "throw an exception if the message type is invalid" >> {
      val key = Array[Byte](1, 'Z', 0, 0, 0, 0, 0, 0, 0, 0)
      val msg = "garbage".getBytes(StandardCharsets.UTF_8)
      foreach(serializers) { serializer =>
        serializer.deserialize(key, msg) must throwAn[IllegalArgumentException]
      }
    }

    "throw an exception if the message cannot be decoded" >> {
      val key = Array[Byte](1, 'C', 1, 2, 3, 4, 5, 6, 7, 8)
      val msg = "garbage".getBytes(StandardCharsets.UTF_8)
      foreach(serializers) { serializer =>
        serializer.deserialize(key, msg) must throwAn[IllegalArgumentException]
      }
    }
  }
}
