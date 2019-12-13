/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}
import java.{lang, util}

import org.apache.avro.Schema
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SchemaBuilder
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureUtilsTest extends Specification {

  "AvroSimpleFeatureUtils" should {

    "encode and decode lists" >> {
      "that are null" >> {
        val encoded = AvroSimpleFeatureUtils.encodeList(null, classOf[String])
        val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
        decoded must beNull
      }
      "that are empty" >> {
        val orig = List.empty[Any]
        val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[String])
        val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
        decoded.asScala mustEqual orig
      }
      "of booleans" >> {
        "in scala" >> {
          val orig = List(true, false, false, true)
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[Boolean])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(lang.Boolean.TRUE, lang.Boolean.TRUE, lang.Boolean.FALSE, lang.Boolean.FALSE)
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[java.lang.Boolean])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of ints" >> {
        "in scala" >> {
          val orig = List(1, 2, 3, 4)
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[Int])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3), Integer.valueOf(4))
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[java.lang.Integer])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of longs" >> {
        "in scala" >> {
          val orig = List(1L, 2L, 3L, 4L)
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[Long])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(lang.Long.valueOf(1), lang.Long.valueOf(2),
            lang.Long.valueOf(3), lang.Long.valueOf(4))
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[java.lang.Long])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of strings" >> {
        "in scala" >> {
          val orig = List("1", "two", "three", "four")
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[String])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(new lang.String("1"), new lang.String("two"),
            new lang.String("three"), new lang.String("four"))
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[String])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "that are empty" >> {
          val orig = List("", "", "non-empty", "")
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[String])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of doubles" >> {
        "in scala" >> {
          val orig = List(1.0, 2.0, 3.0, 4.0)
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[Double])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(lang.Double.valueOf(1.0), lang.Double.valueOf(2.0),
            lang.Double.valueOf(3.0), lang.Double.valueOf(4.0))
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[java.lang.Double])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of floats" >> {
        "in scala" >> {
          val orig = List(1.0f, 2.0f, 3.0f, 4.0f)
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[Float])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = List(lang.Float.valueOf(1.0f), lang.Float.valueOf(2.0f),
            lang.Float.valueOf(3.0f), lang.Float.valueOf(4.0f))
          val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[java.lang.Float])
          val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of dates" >> {
        val orig = List(new Date(0), new Date(), new Date(99999))
        val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[Date])
        val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
        decoded.asScala mustEqual orig
      }
      "of UUIDs" >> {
        val orig = List(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
        val encoded = AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[UUID])
        val decoded = AvroSimpleFeatureUtils.decodeList(encoded)
        decoded.asScala mustEqual orig
      }
    }

    "encode and decode maps" >> {
      "that are null" >> {
        val encoded = AvroSimpleFeatureUtils.encodeMap(null, classOf[String], classOf[String])
        val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
        decoded must beNull
      }
      "that are empty" >> {
        val orig = Map.empty[Any, Any]
        val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[String], classOf[String])
        val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
        decoded.asScala mustEqual orig
      }
      "of booleans" >> {
        "in scala" >> {
          val orig = Map(true -> true, false -> true, false -> false, true -> false)
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[Boolean], classOf[Boolean])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            lang.Boolean.TRUE -> lang.Boolean.TRUE,
            lang.Boolean.TRUE -> lang.Boolean.FALSE,
            lang.Boolean.FALSE -> lang.Boolean.TRUE,
            lang.Boolean.FALSE -> lang.Boolean.FALSE
          )
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava,
            classOf[java.lang.Boolean], classOf[java.lang.Boolean])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of ints" >> {
        "in scala" >> {
          val orig = Map(1 -> 2, 2 -> 3, 3 -> 4, 4 -> 1)
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[Int], classOf[Int])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            Integer.valueOf(1) -> Integer.valueOf(4),
            Integer.valueOf(2) -> Integer.valueOf(3),
            Integer.valueOf(3) -> Integer.valueOf(1),
            Integer.valueOf(4) -> Integer.valueOf(2)
          )
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[Integer], classOf[Integer])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of longs" >> {
        "in scala" >> {
          val orig = Map(1L -> 2L, 2L -> 4L, 3L -> 3L, 4L -> 1L)
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[Long], classOf[Long])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            lang.Long.valueOf(1) -> lang.Long.valueOf(1),
            lang.Long.valueOf(2) -> lang.Long.valueOf(2),
            lang.Long.valueOf(3) -> lang.Long.valueOf(3),
            lang.Long.valueOf(4) -> lang.Long.valueOf(4)
          )
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava,
            classOf[java.lang.Long], classOf[java.lang.Long])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of strings" >> {
        "in scala" >> {
          val orig = Map("1" -> "one", "two" -> "2", "three" -> "3", "four" -> "4")
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[String], classOf[String])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            new lang.String("1") -> new lang.String("one"),
            new lang.String("two") -> new lang.String("2"),
            new lang.String("three") -> new lang.String("3"),
            new lang.String("four") -> new lang.String("4")
          )
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava,
            classOf[java.lang.String], classOf[java.lang.String])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "that are empty" >> {
          val orig = Map("" -> "", "two" -> "")
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[String], classOf[String])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of doubles" >> {
        "in scala" >> {
          val orig = Map(1.0 -> 2.0, 2.0 -> 3.0, 3.0 -> 4.0, 4.0 -> 1.0)
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[Double], classOf[Double])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            lang.Double.valueOf(1.0) -> lang.Double.valueOf(1.0),
            lang.Double.valueOf(2.0) -> lang.Double.valueOf(4.0),
            lang.Double.valueOf(3.0) -> lang.Double.valueOf(3.0),
            lang.Double.valueOf(4.0) -> lang.Double.valueOf(2.0)
          )
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava,
            classOf[java.lang.Double], classOf[java.lang.Double])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of floats" >> {
        "in scala" >> {
          val orig = Map(1.0f -> 1.1f, 2.0f -> 2.1f, 3.0f -> 3.1f, 4.0f -> 4.1f)
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[Float], classOf[Float])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
        "in java" >> {
          val orig = Map(
            lang.Float.valueOf(1.0f) -> lang.Float.valueOf(0.9f),
            lang.Float.valueOf(2.0f) -> lang.Float.valueOf(1.9f),
            lang.Float.valueOf(3.0f) -> lang.Float.valueOf(2.9f),
            lang.Float.valueOf(4.0f) -> lang.Float.valueOf(3.9f)
          )
          val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava,
            classOf[java.lang.Float], classOf[java.lang.Float])
          val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
          decoded.asScala mustEqual orig
        }
      }
      "of dates" >> {
        val orig = Map(
          new Date(0) -> new Date(),
          new Date() -> new Date(99999),
          new Date(99999) -> new Date(0)
        )
        val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[Date], classOf[Date])
        val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
        decoded.asScala mustEqual orig
      }

      "of UUIDs" >> {
        val orig = Map(
          UUID.randomUUID() -> UUID.randomUUID(),
          UUID.fromString("12345678-1234-1234-1234-123456781234") -> UUID.fromString("00000000-0000-0000-0000-000000000000"),
          UUID.randomUUID() -> UUID.randomUUID()
        )
        val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[UUID], classOf[UUID])
        val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
        decoded.asScala mustEqual orig
      }

      "of strings to bytes" >> {
        val one = "235236523\u0000\u0002\u4545!!!!".getBytes(StandardCharsets.UTF_16BE)
        val two = Array(1.toByte, 244.toByte, 55.toByte)
        val orig = Map("1" -> one, "2" -> two)
        val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[String], classOf[Array[Byte]])
        val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
        util.Arrays.equals(decoded.get("1").asInstanceOf[Array[Byte]], one) must beTrue
        util.Arrays.equals(decoded.get("2").asInstanceOf[Array[Byte]], two) must beTrue
      }

      "of mixed keys and values" >> {
        val orig = Map("key1" -> new Date(0), "key2" -> new Date(), "key3" -> new Date(99999))
        val encoded = AvroSimpleFeatureUtils.encodeMap(orig.asJava, classOf[String], classOf[Date])
        val decoded = AvroSimpleFeatureUtils.decodeMap(encoded)
        decoded.asScala mustEqual orig
      }
    }

    "fail with mixed classes" >> {
      "in lists" >> {
        val orig = List(1, "2")
        AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[String]) must throwAn[ClassCastException]
      }
      "in map keys" >> {
        val orig = Map("key1" -> 1, 1.0 -> 2)
        AvroSimpleFeatureUtils.encodeMap(orig.asJava,
          classOf[String], classOf[Int]) must throwAn[ClassCastException]
      }
      "in map values" >> {
        val orig = Map("key1" -> 1, "key2" -> "value2")
        AvroSimpleFeatureUtils.encodeMap(orig.asJava,
          classOf[String], classOf[Int]) must throwAn[ClassCastException]
      }
    }

    "fail-fast with non-primitive items" >> {
      "in lists" >> {
        val orig = List(new Object(), new Object())
        AvroSimpleFeatureUtils.encodeList(orig.asJava, classOf[Object]) must throwAn[IllegalArgumentException]
      }
      "in map keys" >> {
        val orig = Map(new Object() -> 1, new Object() -> 2)
        AvroSimpleFeatureUtils.encodeMap(orig.asJava,
          classOf[Object], classOf[Int]) must throwAn[IllegalArgumentException]
      }
      "in map values" >> {
        val orig = Map(2 -> new Object(), 1 -> new Object())
        AvroSimpleFeatureUtils.encodeMap(orig.asJava,
          classOf[Int], classOf[Object]) must throwAn[IllegalArgumentException]
      }
    }

    "generate Avro schemas" >> {
      "with non-mangled attributes and custom namespaces" >> {
        val sft = SchemaBuilder.builder()
          .addPoint("geom", default = true)
          .addInt("age")
          .build("toavro")

        val expectedSchema = new Schema.Parser().parse("""{"type":"record","name":"toavro","namespace":"test.avro","fields":[{"name":"__version__","type":"int"},{"name":"__fid__","type":"string"},{"name":"geom","type":["bytes","null"]},{"name":"age","type":["int","null"]},{"name":"__userdata__","type":{"type":"array","items":{"type":"record","name":"userDataItem","fields":[{"name":"keyClass","type":"string"},{"name":"key","type":"string"},{"name":"valueClass","type":"string"},{"name":"value","type":"string"}]}}}]}""")
        val schema = AvroSimpleFeatureUtils.generateSchema(sft, withUserData = true, withFeatureId = true, "test.avro")
        expectedSchema must be equalTo schema
      }
    }
  }

}
