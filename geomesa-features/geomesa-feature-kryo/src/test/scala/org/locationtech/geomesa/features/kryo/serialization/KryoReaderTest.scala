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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.SerializationException
import org.locationtech.geomesa.features.serialization.{DatumReader, DatumWriter}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class KryoReaderTest extends Specification {

  sequential

  "KryoReader" should {

    val reader: KryoReader = new KryoReader

    "be able to read" >> {

      val writer: KryoWriter = new KryoWriter

      val output: Output = new Output(1024, -1)
      val input = new Input(Array.empty[Byte])

      def verify[T](value: T, datumWriter: DatumWriter[Output, T], datumReader: DatumReader[Input, T]): MatchResult[Any] = {
        output.clear()
        datumWriter(output, value)

        input.setBuffer(output.toBytes)
        val result = datumReader(input)

        result mustEqual value
      }

      "a String" >> {verify("test", writer.writeString, reader.readString)}
      "an Integer" >> {verify(5, writer.writeInt, reader.readInt)}
      "a positive optimized int" >> {verify(5, writer.writePositiveInt, reader.readPositiveInt)}
      "a Long" >> {verify(10L, writer.writeLong, reader.readLong)}
      "a Float" >> {verify(3.14F, writer.writeFloat, reader.readFloat)}
      "a Double" >> {verify(3.14D, writer.writeDouble, reader.readDouble)}
      "a Boolean" >> {verify(true, writer.writeBoolean, reader.readBoolean)}
      "a Date" >> {verify(new java.util.Date(), writer.writeDate, reader.readDate)}
      "an array of bytes" >> {verify(Array(1.toByte, 2.toByte, 3.toByte), writer.writeBytes, reader.readBytes)}

      "a nullable value" >> {
        val datumReader = reader.readNullable(reader.readString)
        val datumWriter = writer.writeNullable(writer.writeString)

        "that is not null" >> {verify("not null", datumWriter, datumReader)}
        "that is null" >> {verify(null, datumWriter, datumReader)}
      }

      "a UUID" >> {verify(UUID.randomUUID(), writer.writeUUID, reader.readUUID)}

      "a hint key" >> {
        "that is supported" >> {
          verify(Hints.USE_PROVIDED_FID, writer.writeHintKey, reader.readHintKey)
        }
        "that is not supported" >> {
          // force write a unknown hint key
          output.clear()
          output.writeString("BAD_KEY")
          input.setBuffer(output.toBytes)

          // read should fail
          reader.readHintKey(input) must throwA[SerializationException]("Unknown Key ID: 'BAD_KEY'")
        }
      }

      "a list" >> {
        val datumReader = reader.readList(reader.readString)
        val datumWriter = writer.writeList(writer.writeString)

        "that is null" >> {verify(null, datumWriter, datumReader)}

        "that is empty" >> {
          val list: java.util.List[String] = java.util.Collections.emptyList()
          verify(list, datumWriter, datumReader)
        }

        "that is not empty" >> {
          val list = Seq("A", "B", "C", "D").toList.asJava
          verify(list, datumWriter, datumReader)
        }
      }

      "a map" >> {
        val datumReader = reader.readMap(reader.readString, reader.readInt)
        val datumWriter = writer.writeMap(writer.writeString, writer.writeInt)

        "that is null" >> {verify(null, datumWriter, datumReader)}

        "that is empty" >> {
          val map: java.util.Map[String, Int] = java.util.Collections.emptyMap()
          verify(map, datumWriter, datumReader)
        }

        "that is not empty" >> {
          val map = Map(
            "A" -> 1,
            "B" -> 2,
            "C" -> 3,
            "D" -> 4
          ).asJava
          verify(map, datumWriter, datumReader)
        }
      }

      "a generic value" >> {
        val datumReader = reader.readGeneric(0)
        val datumWriter = writer.writeGeneric

        "that is not null" >> {verify(6.asInstanceOf[AnyRef], datumWriter, datumReader)}
        "that is null" >> {verify(null.asInstanceOf[AnyRef], datumWriter, datumReader)}
      }

      "a generic map" >> {
        val datumReader = reader.readGenericMap(1)
        val datumWriter = writer.writeGenericMap

        "that is empty" >> {
          val map = Map.empty[AnyRef, AnyRef].asJava
          verify(map, datumWriter, datumReader)
        }

        "that is not empty" >> {
          val map = Map(
            "visibility" -> "USER|ADMIN",
            Hints.USE_PROVIDED_FID -> false.asInstanceOf[AnyRef],
            "date" -> new java.util.Date(),
            5.asInstanceOf[AnyRef] -> 5.0D.asInstanceOf[AnyRef]
          ).asJava
          verify(map, datumWriter, datumReader)
        }
      }

      "a point" >> {
        val point = WKTUtils.read("POINT(55.0 49.0)")
        verify(point, writer.writeGeometry, reader.readGeometryDirectly)
      }

      "a line string" >> {
        val point = WKTUtils.read("LINESTRING(0 2, 2 0, 8 6)")
        verify(point, writer.writeGeometry, reader.readGeometryDirectly)
      }

      "a polygon" >> {
        val point = WKTUtils.read("POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))")
        verify(point, writer.writeGeometry, reader.readGeometryDirectly)
      }

      "a polygon with an inner ring" >> {
        val point = WKTUtils.read("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))")
        verify(point, writer.writeGeometry, reader.readGeometryDirectly)
      }

      "a multi point" >> {
        val point = WKTUtils.read("MULTIPOINT(0 0, 2 2)")
        verify(point, writer.writeGeometry, reader.readGeometryDirectly)
      }

      "a multi line string" >> {
        val point = WKTUtils.read("MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))")
        verify(point, writer.writeGeometry, reader.readGeometryDirectly)
      }

      "a multi polygon" >> {
        val point = WKTUtils.read("MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))")
        verify(point, writer.writeGeometry, reader.readGeometryDirectly)
      }

      "a geomerty collection" >> {
        val point = WKTUtils.read("GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))")
        verify(point, writer.writeGeometry, reader.readGeometryDirectly)
      }

      "binary geometry" >> {
        val geom = WKTUtils.read("POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))")
        val bytes = WKBUtils.write(geom)

        output.clear()
        writer.writeBytes(output, bytes)
        input.setBuffer(output.toBytes)

        val result = reader.readGeometryAsWKB(input)
        result mustEqual geom
      }

      "empty binary geometry" >> {
        val bytes = Array.empty[Byte]

        output.clear()
        writer.writeBytes(output, bytes)
        input.setBuffer(output.toBytes)

        val result = reader.readGeometryAsWKB(input)
        result must beNull
      }
    }

    "be able to select a datum reader" >> {

      "for a String" >> {
        val result = reader.selectReader(classOf[String], 0)
        result must be equalTo reader.readString
      }

      "for an Integer" >> {
        val result = reader.selectReader(classOf[java.lang.Integer], 0)
        result must be equalTo reader.readInt.asInstanceOf[DatumReader[Input, AnyRef]]
      }

      "for a Long" >> {
        val result = reader.selectReader(classOf[java.lang.Long], 0)
        result must be equalTo reader.readLong.asInstanceOf[DatumReader[Input, AnyRef]]
      }

      "for a float" >> {
        val result = reader.selectReader(classOf[java.lang.Float], 0)
        result must be equalTo reader.readFloat.asInstanceOf[DatumReader[Input, AnyRef]]
      }

      "for a double" >> {
        val result = reader.selectReader(classOf[java.lang.Double], 0)
        result must be equalTo reader.readDouble.asInstanceOf[DatumReader[Input, AnyRef]]
      }

      "for a boolean" >> {
        val result = reader.selectReader(classOf[java.lang.Boolean], 0)
        result must be equalTo reader.readBoolean.asInstanceOf[DatumReader[Input, AnyRef]]
      }

      "for a date" >> {
        val result = reader.selectReader(classOf[java.util.Date], 0)
        result must be equalTo reader.readDate
      }

      "for a UUID" >> {
        val result = reader.selectReader(classOf[UUID], 0)
        result must not(beNull)
      }

      "for a Geometry" >> {
        val result = reader.selectReader(classOf[Geometry], 0)
        result must not(beNull)
      }

      "for a hint key" >> {
        val result = reader.selectReader(classOf[Hints.Key], 0)
        result must not(beNull)
      }

      "for a List" >> {
        "with necessary metadata" >> {
          val metadata = Map(USER_DATA_LIST_TYPE -> classOf[java.util.Date]).asJava
          val result = reader.selectReader(classOf[java.util.List[java.util.Date]], 0, metadata)
          result must not(beNull)
        }

        "with missing metadata" >> {
          reader.selectReader(classOf[java.util.List[java.util.Date]], 0) must throwA[NullPointerException]
        }
      }

      "for a Map" >> {
        "with necessary metadata" >> {
          val metadata = Map(
            USER_DATA_MAP_KEY_TYPE -> classOf[String],
            USER_DATA_MAP_VALUE_TYPE -> classOf[Geometry]
          ).asJava
          val result = reader.selectReader(classOf[java.util.Map[String, Geometry]], 0, metadata)
          result must not(beNull)
        }

        "with missing metadata" >> {
          reader.selectReader(classOf[java.util.Map[String, Geometry]], 0) must throwA[NullPointerException]
        }
      }
    }

    "fail to select a datum reader when class is unknown" >> {
      reader.selectReader(classOf[java.util.Set[Geometry]], 0) must throwA[IllegalArgumentException]
    }

    "read geometry directly for version 1" >> {
      val result = reader.selectGeometryReader(1)
      result mustEqual reader.readGeometryDirectly
    }

    "read geometry as WKB for version 0" >> {
      val result = reader.selectGeometryReader(0)
      result mustEqual reader.readGeometryAsWKB
    }
  }
}
