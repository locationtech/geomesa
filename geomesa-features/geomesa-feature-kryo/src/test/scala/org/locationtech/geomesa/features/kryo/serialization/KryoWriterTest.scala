/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.features.kryo.serialization

import java.util.UUID

import com.esotericsoftware.kryo.io.Output
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.SerializationException
import org.locationtech.geomesa.features.serialization.{DatumWriter, AbstractWriter, HintKeySerialization}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class KryoWriterTest extends Specification with Mockito {

  sequential

  "KryoWriter" should {

    val writer = new KryoWriter

    "be able to write" >> {

      val output = mock[Output]

      "a string" >> {
        writer.writeString(output, "foo")
        there was one(output).writeString("foo")
      }

      "an int" >> {
        writer.writeInt(output, 5)
        there was one(output).writeInt(5)
      }

      "a positive optimized int" >> {
        writer.writePositiveInt(output, 5)
        there was one(output).writeInt(5, true)
      }

      "a long" >> {
        writer.writeLong(output, 8L)
        there was one(output).writeLong(8L)
      }

      "a float" >> {
        val value = 3.14F
        writer.writeFloat(output, value)
        there was one(output).writeFloat(value)
      }

      "a double" >> {
        val value = 3.14D
        writer.writeDouble(output, value)
        there was one(output).writeDouble(value)
      }

      "a boolean" >> {
        writer.writeBoolean(output, true)
        there was one(output).writeBoolean(true)
      }

      "a date" >> {
        val date = new java.util.Date(123456789L)
        writer.writeDate(output, date)
        there was one(output).writeLong(123456789L)
      }

      "an array of bytes" >> {
        val bytes: Array[Byte] = Array(1.toByte, 2.toByte, 3.toByte)
        writer.writeBytes(output, bytes)
        there was one(output).writeInt(3, true) andThen one(output).writeBytes(bytes)
      }

      "a UUID" >> {
        val uuid = UUID.randomUUID()
        writer.writeUUID(output, uuid)
        there was one(output).writeLong(uuid.getMostSignificantBits) andThen one(output).writeLong(uuid.getLeastSignificantBits)
      }

      "a hint key" >> {
        "that is supported" >> {
          writer.writeHintKey(output, Hints.USE_PROVIDED_FID)
          val expected = HintKeySerialization.keyToId(Hints.USE_PROVIDED_FID)

          there was one(output).writeString(expected)
        }
      }

      "a nullable value" >> {

        val datumWriter = writer.writeNullable(writer.writeInt).asInstanceOf[DatumWriter[Output, AnyRef]]

        "that is not null" >> {
          datumWriter(output, 6.asInstanceOf[AnyRef])
          there was one(output).writeByte(KryoSerialization.NON_NULL_MARKER_BYTE) andThen one(output).writeInt(6)
        }

        "that is null" >> {
          datumWriter(output, null)
          there was one(output).writeByte(KryoSerialization.NULL_MARKER_BYTE) andThen noMoreCallsTo(output)
        }

        "a generic value" >> {

          "that is not null" >> {
            val value: AnyRef = 16.asInstanceOf[AnyRef]
            writer.writeGeneric(output, value)
            there was one(output).writeString("java.lang.Integer") andThen one(output).writeInt(16)
          }

          "that is null" >> {
            val value: AnyRef = null
            writer.writeGeneric(output, value)
            there was one(output).writeString(AbstractWriter.NULL_MARKER_STR)
          }
        }
      }
    }

    "fail to write" >> {
      "an unsupported hint key" >> {
        val output = mock[Output]
        val key: Hints.Key = mock[Hints.Key]

        writer.writeHintKey(output, key) must throwA[SerializationException](s"Unknown Key: '$key'")
      }
    }

    "be able to select a datum writer" >> {

      "for a String" >> {
        val result = writer.selectWriter(classOf[String])
        result must be equalTo writer.writeString
      }

      "for an Integer" >> {
        val result = writer.selectWriter(classOf[java.lang.Integer])
        result must be equalTo writer.writeInt.asInstanceOf[DatumWriter[Output, AnyRef]]
      }

      "for a Long" >> {
        val result = writer.selectWriter(classOf[java.lang.Long])
        result must be equalTo writer.writeLong.asInstanceOf[DatumWriter[Output, AnyRef]]
      }

      "for a Float" >> {
        val result = writer.selectWriter(classOf[java.lang.Float])
        result must be equalTo writer.writeFloat.asInstanceOf[DatumWriter[Output, AnyRef]]
      }

      "for a Double" >> {
        val result = writer.selectWriter(classOf[java.lang.Double])
        result must be equalTo writer.writeDouble.asInstanceOf[DatumWriter[Output, AnyRef]]
      }

      "for a Boolean" >> {
        val result = writer.selectWriter(classOf[java.lang.Boolean])
        result must be equalTo writer.writeBoolean.asInstanceOf[DatumWriter[Output, AnyRef]]
      }

      "for a Date" >> {
        val result = writer.selectWriter(classOf[java.util.Date])
        result must be equalTo writer.writeDate
      }

      "for a UUID" >> {
        val result: DatumWriter[Output, UUID] = writer.selectWriter(classOf[java.util.UUID])
        result must not(beNull)
      }

      "for a Geometry" >> {
        val result = writer.selectWriter(classOf[Geometry])
        result must be equalTo writer.writeGeometry
      }

      "for a Hint Key" >> {
        val result = writer.selectWriter(classOf[Hints.Key])
        result must be equalTo writer.writeHintKey
      }

      "for a List" >> {
        "with necessary metadata" >> {
          val metadata = Map(USER_DATA_LIST_TYPE -> classOf[java.util.Date]).asJava
          val result: DatumWriter[Output, List[java.util.Date]] = writer.selectWriter(classOf[java.util.List[java.util.Date]], metadata)
          result must not(beNull)
        }

        "with missing metadata" >> {
          writer.selectWriter(classOf[java.util.List[java.util.Date]]) must throwA[NullPointerException]
        }
      }

      "for a Map" >> {
        "with necessary metadata" >> {
          val metadata = Map(
            USER_DATA_MAP_KEY_TYPE -> classOf[String],
            USER_DATA_MAP_VALUE_TYPE -> classOf[Geometry]
          ).asJava
          val result: DatumWriter[Output, Map[String, Geometry]] = writer.selectWriter(classOf[java.util.Map[String, Geometry]], metadata)
          result must not(beNull)
        }

        "with missing metadata" >> {
          writer.selectWriter(classOf[java.util.Map[String, Geometry]]) must throwA[NullPointerException]
        }
      }
    }

    "fail to select a datum writer when class is unknown" >> {
      writer.selectWriter(classOf[java.util.Set[Geometry]]) must throwA[IllegalArgumentException]
    }
  }
}
