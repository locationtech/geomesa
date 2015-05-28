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

package org.locationtech.geomesa.jobs.mapreduce

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.geotools.feature.simple.SimpleFeatureImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeature
import org.locationtech.geomesa.features.{ScalaSimpleFeature, ScalaSimpleFeatureFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureSerializationTest extends Specification {

  "SimpleFeatureSerialization" should {

    "support simple feature serialization" in {
      val serialization = new SimpleFeatureSerialization
      serialization.accept(classOf[ScalaSimpleFeature]) must beTrue
      serialization.accept(classOf[SimpleFeature]) must beTrue
      serialization.accept(classOf[AvroSimpleFeature]) must beTrue
      serialization.accept(classOf[SimpleFeatureImpl]) must beTrue

      serialization.getSerializer(classOf[SimpleFeature]) must not beNull;
      serialization.getDeserializer(classOf[SimpleFeature]) must not beNull;
    }

    "serialize and deserialize a simple feature" in {
      val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
      val sf = ScalaSimpleFeatureFactory.buildFeature(sft, Seq("myname", "2014-01-10T00:00:00.000Z", "POINT(45 46)"), "fid-1")

      val serialization = new SimpleFeatureSerialization
      val serializer = serialization.getSerializer(classOf[SimpleFeature])
      val deserializer = serialization.getDeserializer(classOf[SimpleFeature])

      val out = new ByteArrayOutputStream()
      serializer.open(out)
      serializer.serialize(sf)
      serializer.close()

      val serialized = out.toByteArray
      val in = new ByteArrayInputStream(serialized)

      deserializer.open(in)
      val deserialized = deserializer.deserialize(null)
      deserializer.close()

      deserialized mustEqual(sf)
      deserialized.getFeatureType mustEqual(sft)
    }

    "serialize and deserialize a simple feature with a long spec" in {
      val sft = SimpleFeatureTypes.createType("test", "dtg:Date,attr1:String,attr2:String:index=full," +
          "lat:Double,lon:Double,attr3:Integer,attr4:Integer,attr5:Integer,attr6:String:index=join," +
          "attr7:String,attr8:String,attr9:String,attr10:String,*geom:Point:srid=4326:index=full:index-value=true")
      val sf = ScalaSimpleFeatureFactory.buildFeature(sft, Seq("2014-01-10T00:00:00.000Z"), "fid-1")
      sf.setAttribute("geom", "POINT(45 46)")

      val serialization = new SimpleFeatureSerialization
      val serializer = serialization.getSerializer(classOf[SimpleFeature])
      val deserializer = serialization.getDeserializer(classOf[SimpleFeature])

      val out = new ByteArrayOutputStream()
      serializer.open(out)
      serializer.serialize(sf)
      serializer.close()

      val serialized = out.toByteArray
      val in = new ByteArrayInputStream(serialized)

      deserializer.open(in)
      val deserialized = deserializer.deserialize(null)
      deserializer.close()

      deserialized mustEqual(sf)
      deserialized.getFeatureType mustEqual(sft)
    }
  }

}
