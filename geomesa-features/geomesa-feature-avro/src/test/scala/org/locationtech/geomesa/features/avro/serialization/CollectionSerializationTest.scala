/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serialization

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class CollectionSerializationTest extends Specification {

  "CollectionSerialization" should {

    "encode and decode lists" >> {
      "that are null" >> {
        val encoded = CollectionSerialization.encodeList(null, "string")
        val decoded = CollectionSerialization.decodeList(encoded)
        decoded must beNull
      }
      "that are empty" >> {
        val orig = List.empty[Any]
        val encoded = CollectionSerialization.encodeList(orig.asJava, "string")
        val decoded = CollectionSerialization.decodeList(encoded)
        decoded.asScala mustEqual orig
      }
      "of booleans" >> {
        "in scala" >> {
          val orig = List(true, false, false, true)
          val encoded = CollectionSerialization.encodeList(orig.asJava, "boolean")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(Boolean.box(true), Boolean.box(true), Boolean.box(false), Boolean.box(false))
          val encoded = CollectionSerialization.encodeList(orig.asJava, "boolean")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of ints" >> {
        "in scala" >> {
          val orig = List(1, 2, 3, 4)
          val encoded = CollectionSerialization.encodeList(orig.asJava, "int")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(Int.box(1), Int.box(2), Int.box(3), Int.box(4))
          val encoded = CollectionSerialization.encodeList(orig.asJava, "integer")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of longs" >> {
        "in scala" >> {
          val orig = List(1L, 2L, 3L, 4L)
          val encoded = CollectionSerialization.encodeList(orig.asJava, "long")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(Long.box(1), Long.box(2), Long.box(3), Long.box(4))
          val encoded = CollectionSerialization.encodeList(orig.asJava, "long")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of strings" >> {
        "in scala" >> {
          val orig = List("1", "two", "three", "four")
          val encoded = CollectionSerialization.encodeList(orig.asJava, "string")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "that are empty" >> {
          val orig = List("", "", "non-empty", "")
          val encoded = CollectionSerialization.encodeList(orig.asJava, "string")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of doubles" >> {
        "in scala" >> {
          val orig = List(1.0, 2.0, 3.0, 4.0)
          val encoded = CollectionSerialization.encodeList(orig.asJava, "double")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(Double.box(1.0), Double.box(2.0), Double.box(3.0), Double.box(4.0))
          val encoded = CollectionSerialization.encodeList(orig.asJava, "double")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of floats" >> {
        "in scala" >> {
          val orig = List(1.0f, 2.0f, 3.0f, 4.0f)
          val encoded = CollectionSerialization.encodeList(orig.asJava, "float")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(Float.box(1.0f), Float.box(2.0f), Float.box(3.0f), Float.box(4.0f))
          val encoded = CollectionSerialization.encodeList(orig.asJava, "float")
          val decoded = CollectionSerialization.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of dates" >> {
        val orig = List(new Date(0), new Date(), new Date(99999))
        val encoded = CollectionSerialization.encodeList(orig.asJava, "date")
        val decoded = CollectionSerialization.decodeList(encoded)
        decoded.asScala mustEqual orig
      }
      "of UUIDs" >> {
        val orig = List(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
        val encoded = CollectionSerialization.encodeList(orig.asJava, "uuid")
        val decoded = CollectionSerialization.decodeList(encoded)
        decoded.asScala mustEqual orig
      }
    }

    "encode and decode maps" >> {
      "that are null" >> {
        val encoded = CollectionSerialization.encodeMap(null, "string", "string")
        val decoded = CollectionSerialization.decodeMap(encoded)
        decoded must beNull
      }
      "that are empty" >> {
        val orig = Map.empty[Any, Any]
        val encoded = CollectionSerialization.encodeMap(orig.asJava, "string", "string")
        val decoded = CollectionSerialization.decodeMap(encoded)
        decoded.asScala mustEqual orig
      }
      "of booleans" >> {
        "in scala" >> {
          val orig = Map(true -> true, false -> true, false -> false, true -> false)
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "boolean", "boolean")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            Boolean.box(true) -> Boolean.box(true),
            Boolean.box(true) -> Boolean.box(false),
            Boolean.box(false) -> Boolean.box(true),
            Boolean.box(false) -> Boolean.box(false)
          )
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "boolean", "boolean")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of ints" >> {
        "in scala" >> {
          val orig = Map(1 -> 2, 2 -> 3, 3 -> 4, 4 -> 1)
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "int", "int")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            Int.box(1) -> Int.box(4),
            Int.box(2) -> Int.box(3),
            Int.box(3) -> Int.box(1),
            Int.box(4) -> Int.box(2)
          )
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "integer", "integer")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of longs" >> {
        "in scala" >> {
          val orig = Map(1L -> 2L, 2L -> 4L, 3L -> 3L, 4L -> 1L)
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "long", "long")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            Long.box(1) -> Long.box(1),
            Long.box(2) -> Long.box(2),
            Long.box(3) -> Long.box(3),
            Long.box(4) -> Long.box(4)
          )
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "long", "long")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of strings" >> {
        "in scala" >> {
          val orig = Map("1" -> "one", "two" -> "2", "three" -> "3", "four" -> "4")
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "string", "string")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "that are empty" >> {
          val orig = Map("" -> "", "two" -> "")
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "string", "string")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of doubles" >> {
        "in scala" >> {
          val orig = Map(1.0 -> 2.0, 2.0 -> 3.0, 3.0 -> 4.0, 4.0 -> 1.0)
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "double", "double")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            Double.box(1.0) -> Double.box(1.0),
            Double.box(2.0) -> Double.box(4.0),
            Double.box(3.0) -> Double.box(3.0),
            Double.box(4.0) -> Double.box(2.0)
          )
          val encoded = CollectionSerialization.encodeMap(orig.asJava,
            "double", "double")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of floats" >> {
        "in scala" >> {
          val orig = Map(1.0f -> 1.1f, 2.0f -> 2.1f, 3.0f -> 3.1f, 4.0f -> 4.1f)
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "float", "float")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            Float.box(1.0f) -> Float.box(0.9f),
            Float.box(2.0f) -> Float.box(1.9f),
            Float.box(3.0f) -> Float.box(2.9f),
            Float.box(4.0f) -> Float.box(3.9f)
          )
          val encoded = CollectionSerialization.encodeMap(orig.asJava, "float", "float")
          val decoded = CollectionSerialization.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of dates" >> {
        val orig = Map(
          new Date(0) -> new Date(),
          new Date() -> new Date(99999),
          new Date(99999) -> new Date(0)
        )
        val encoded = CollectionSerialization.encodeMap(orig.asJava, "date", "date")
        val decoded = CollectionSerialization.decodeMap(encoded)
        decoded.asScala mustEqual orig
      }

      "of UUIDs" >> {
        val orig = Map(
          UUID.randomUUID() -> UUID.randomUUID(),
          UUID.fromString("12345678-1234-1234-1234-123456781234") -> UUID.fromString("00000000-0000-0000-0000-000000000000"),
          UUID.randomUUID() -> UUID.randomUUID()
        )
        val encoded = CollectionSerialization.encodeMap(orig.asJava, "uuid", "uuid")
        val decoded = CollectionSerialization.decodeMap(encoded)
        decoded.asScala mustEqual orig
      }

      "of strings to bytes" >> {
        val one = "235236523\u0000\u0002\u4545!!!!".getBytes(StandardCharsets.UTF_16BE)
        val two = Array(1.toByte, 244.toByte, 55.toByte)
        val orig = Map("1" -> one, "2" -> two)
        val encoded = CollectionSerialization.encodeMap(orig.asJava, "string", "byte[]")
        val decoded = CollectionSerialization.decodeMap(encoded)
        java.util.Arrays.equals(decoded.get("1").asInstanceOf[Array[Byte]], one) must beTrue
        java.util.Arrays.equals(decoded.get("2").asInstanceOf[Array[Byte]], two) must beTrue
      }

      "of mixed keys and values" >> {
        val orig = Map("key1" -> new Date(0), "key2" -> new Date(), "key3" -> new Date(99999))
        val encoded = CollectionSerialization.encodeMap(orig.asJava, "string", "date")
        val decoded = CollectionSerialization.decodeMap(encoded)
        decoded.asScala mustEqual orig
      }
    }

    "fail with mixed classes" >> {
      "in lists" >> {
        val orig = List(1, "2")
        CollectionSerialization.encodeList(orig.asJava, "string") must throwAn[ClassCastException]
      }
      "in map keys" >> {
        val orig = Map("key1" -> 1, 1.0 -> 2)
        CollectionSerialization.encodeMap(orig.asJava, "string", "int") must throwAn[ClassCastException]
      }
      "in map values" >> {
        val orig = Map("key1" -> 1, "key2" -> "value2")
        CollectionSerialization.encodeMap(orig.asJava, "string", "int") must throwAn[ClassCastException]
      }
    }

    "fail-fast with non-primitive items" >> {
      "in lists" >> {
        val orig = List(new Object(), new Object())
        CollectionSerialization.encodeList(orig.asJava, "object") must throwAn[IllegalArgumentException]
      }
      "in map keys" >> {
        val orig = Map(new Object() -> 1, new Object() -> 2)
        CollectionSerialization.encodeMap(orig.asJava, "object", "int") must throwAn[IllegalArgumentException]
      }
      "in map values" >> {
        val orig = Map(2 -> new Object(), 1 -> new Object())
        CollectionSerialization.encodeMap(orig.asJava, "int", "object") must throwAn[IllegalArgumentException]
      }
    }
  }

}
