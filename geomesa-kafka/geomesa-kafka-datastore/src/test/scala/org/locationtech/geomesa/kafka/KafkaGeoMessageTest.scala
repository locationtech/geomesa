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
package org.locationtech.geomesa.kafka

import java.nio.charset.StandardCharsets

import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroFeatureEncoder
import org.locationtech.geomesa.feature.EncodingOption.EncodingOptions
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class KafkaGeoMessageTest extends Specification with Mockito {

  sequential

  type ProducerMsg = KafkaGeoMessageEncoder.MSG
  type ConsumerMsg = KafkaGeoMessageDecoder#MSG
  
  val topic = "test_topic"
  val schema = SimpleFeatureTypes.createType("KafkaGeoMessageTest", "name:String,*geom:Point:srid=4326")
  
  "Clear" should {

    "be able to be encoded and decoded" >> {
      val msg = Clear

      val encoder = new KafkaGeoMessageEncoder(schema)
      val encoded: ProducerMsg = encoder.encode(topic, msg)

      encoded.hasKey must beTrue
      encoded.key mustEqual "clear".getBytes(StandardCharsets.UTF_8)

      val decoder = new KafkaGeoMessageDecoder(schema)
      val decoded: KafkaGeoMessage = decoder.decode(encoded)
      decoded mustEqual msg
    }
  }

  "Delete" should {

    "be able to be encoded and decoded" >> {
      val id = "test_id"
      val msg = Delete(id)

      val encoder = new KafkaGeoMessageEncoder(schema)
      val encoded: ProducerMsg = encoder.encode(topic, msg)

      encoded.hasKey must beTrue
      encoded.key mustEqual "delete".getBytes(StandardCharsets.UTF_8)
      encoded.message mustEqual id.getBytes(StandardCharsets.UTF_8)

      val decoder = new KafkaGeoMessageDecoder(schema)
      val decoded: KafkaGeoMessage = decoder.decode(encoded)
      decoded mustEqual msg
    }
  }

  "CreateOrUpdate" should {

    "be able to be encoded and decoded" >> {
      val id = "test_id"
      val sfVals = List[AnyRef]("foo", WKTUtils.read("POINT(1 -1)"))
      val sf = new SimpleFeatureImpl(sfVals.asJava, schema, new FeatureIdImpl(id))
      val msg = CreateOrUpdate(sf)

      val expectedEncoded = {
        val sfEncoder = new AvroFeatureEncoder(schema, EncodingOptions.withUserData)
        sfEncoder.encode(sf)
      }

      val encoder = new KafkaGeoMessageEncoder(schema)
      val encoded: ProducerMsg = encoder.encode(topic, msg)

      encoded.hasKey must beFalse
      encoded.message mustEqual expectedEncoded

      val decoder = new KafkaGeoMessageDecoder(schema)
      val decoded: KafkaGeoMessage = decoder.decode(encoded)
      decoded.isInstanceOf[CreateOrUpdate] must beTrue

      val cu = decoded.asInstanceOf[CreateOrUpdate]
      cu.id mustEqual id
      cu.feature.getFeatureType mustEqual schema
      cu.feature.getAttribute("name") mustEqual sfVals(0)
      cu.feature.getAttribute("geom") mustEqual sfVals(1)
    }
  }

  "KafkaGeoMessageDecoder" should {

    "throw an exception if the message cannot be decoded" >> {
      val decoder = new KafkaGeoMessageDecoder(schema)

      val msg = mock[ConsumerMsg]
      msg.key() returns "garbage".getBytes(StandardCharsets.UTF_8)
      msg.message() returns "garbage".getBytes(StandardCharsets.UTF_8)

      decoder.decode(msg) must throwAn[IllegalArgumentException]
    }
  }

  /** The consumer and producer APIs are not symmetric.  Convert a message sent to a producer into a message
    * received by a consumer.
    */
  implicit def transport(pmsg: ProducerMsg): ConsumerMsg = {
    val cmsg: ConsumerMsg = mock[ConsumerMsg]
    cmsg.key() returns pmsg.key
    cmsg.message() returns pmsg.message
    cmsg
  }
}
