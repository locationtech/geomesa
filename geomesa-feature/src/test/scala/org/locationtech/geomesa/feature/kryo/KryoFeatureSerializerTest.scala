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

package org.locationtech.geomesa.feature.kryo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.{Point, Polygon}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ListBuffer
import scala.util.Random

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


    "correctly serialize and deserialize null values" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,l:List,m:Map," +
          "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature("fakeid", sft)

      import scala.collection.JavaConverters._
      "using byte arrays" >> {
        val serializer = KryoFeatureSerializer(sft)

        val serialized = serializer.write(sf)
        val deserialized = serializer.read(serialized)

        deserialized must not beNull;
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes.asScala.foreach(_ must beNull)
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
        deserialized.getAttributes.asScala.foreach(_ must beNull)
        deserialized.getAttributes mustEqual sf.getAttributes
      }
    }
  }

}
