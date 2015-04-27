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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroFeatureEncoder
import org.locationtech.geomesa.feature.EncodingOption.EncodingOptions
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.Matcher
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

  implicit val mockClock = new Clock {
    override val now: Instant = new Instant(ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)).getLong)
  }

  def createSimpleFeature: SimpleFeature = {
    val id = "test_id"
    val sfVals = List[AnyRef]("foo", WKTUtils.read("POINT(1 -1)"))
    new SimpleFeatureImpl(sfVals.asJava, schema, new FeatureIdImpl(id))
  }

  def encodeSF(sf: SimpleFeature): Array[Byte] = {
    val sfEncoder = new AvroFeatureEncoder(schema, EncodingOptions.withUserData)
    sfEncoder.encode(sf)
  }

  "Clear" should {

    val msg = KafkaGeoMessage.clear()

    "use the correct timestamp" >> {
      msg.timestamp mustEqual mockClock.now
    }

    "be able to be encoded and decoded" >> {
      val encoder = new KafkaGeoMessageEncoder(schema)
      val encoded: ProducerMsg = encoder.encode(topic, msg)

      encoded must not(beNull)
      encoded.key mustEqual Array[Byte](1, 'X', 1, 2, 3, 4, 5, 6, 7, 8)
      encoded.message mustEqual Array.empty[Byte]

      val decoder = new KafkaGeoMessageDecoder(schema)
      val decoded: KafkaGeoMessage = decoder.decode(encoded)

      decoded mustEqual msg
    }
  }

  "Delete" should {

    val id = "test_id"
    val msg = KafkaGeoMessage.delete(id)

    "use the correct timestamp" >> {
      msg.timestamp mustEqual mockClock.now
    }

    "be able to be encoded and decoded" >> {
      val encoder = new KafkaGeoMessageEncoder(schema)
      val encoded: ProducerMsg = encoder.encode(topic, msg)

      encoded must not(beNull)
      encoded.key mustEqual Array[Byte](1, 'D', 1, 2, 3, 4, 5, 6, 7, 8)
      encoded.message mustEqual id.getBytes(StandardCharsets.UTF_8)

      val decoder = new KafkaGeoMessageDecoder(schema)
      val decoded: KafkaGeoMessage = decoder.decode(encoded)

      decoded mustEqual msg
    }
  }

  "CreateOrUpdate" should {

    val sf = createSimpleFeature
    val msg = KafkaGeoMessage.createOrUpdate(sf)

    "use the correct timestamp" >> {
      msg.timestamp mustEqual mockClock.now
    }

    "be able to be encoded and decoded" >> {
      val encoder = new KafkaGeoMessageEncoder(schema)
      val encoded: ProducerMsg = encoder.encode(topic, msg)

      encoded must not(beNull)
      encoded.key mustEqual Array[Byte](1, 'C', 1, 2, 3, 4, 5, 6, 7, 8)
      encoded.message mustEqual encodeSF(sf)

      val decoder = new KafkaGeoMessageDecoder(schema)
      val decoded: KafkaGeoMessage = decoder.decode(encoded)

      decoded.isInstanceOf[CreateOrUpdate] must beTrue

      val cu = decoded.asInstanceOf[CreateOrUpdate]
      cu.id mustEqual sf.getID
      cu.feature must equalSF(sf)
    }
  }

  "KafkaGeoMessageDecoder" should {

    "be able to decode version 0 messages" >> {

      val decoder = new KafkaGeoMessageDecoder(schema)

      "of type Clear" >> {
        val key = "clear".getBytes(StandardCharsets.UTF_8)
        val msg = Array.empty[Byte]
        val cmsg = mockConsumerMessage(key, msg)

        val result = decoder.decode(cmsg)
        result must not(beNull)
        result.isInstanceOf[Clear] must beTrue
        result.timestamp mustEqual new Instant(0L)
      }

      "of type Delete" >> {
        val id = "test_id"

        val key = "delete".getBytes(StandardCharsets.UTF_8)
        val msg = id.getBytes(StandardCharsets.UTF_8)
        val cmsg = mockConsumerMessage(key, msg)

        val result = decoder.decode(cmsg)
        result must not(beNull)
        result.isInstanceOf[Delete] must beTrue
        result.timestamp mustEqual new Instant(0L)
        result.asInstanceOf[Delete].id mustEqual id
      }

      "of type CreateOrUpdate" >> {
        val sf = createSimpleFeature
        val key: Array[Byte] = null
        val msg = encodeSF(sf)

        val cmsg = mockConsumerMessage(key, msg)

        val result = decoder.decode(cmsg)
        result must not(beNull)
        result.isInstanceOf[CreateOrUpdate] must beTrue
        result.timestamp mustEqual new Instant(0L)
        result.asInstanceOf[CreateOrUpdate].feature must equalSF(sf)
      }
    }

    "throw an exception if the message cannot be decoded" >> {
      val decoder = new KafkaGeoMessageDecoder(schema)

      val msg = mockConsumerMessage(
        "garbage".getBytes(StandardCharsets.UTF_8),
        "garbage".getBytes(StandardCharsets.UTF_8))

      decoder.decode(msg) must throwAn[IllegalArgumentException]("Unknown message key")
    }

    "throw an exception if the message key lenght is incorrect" >> {
      val decoder = new KafkaGeoMessageDecoder(schema)

      val msg = mockConsumerMessage(
        Array[Byte](1, 1, 1),
        "garbage".getBytes(StandardCharsets.UTF_8))

      decoder.decode(msg) must throwAn[IllegalArgumentException]("Invalid message key")
    }

    "throw an exception if the message type is invalid" >> {
      val decoder = new KafkaGeoMessageDecoder(schema)

      val msg = mockConsumerMessage(
        Array[Byte](1, 'U', 0, 0, 0, 0, 0, 0, 0, 0),
        "garbage".getBytes(StandardCharsets.UTF_8))

      decoder.decode(msg) must throwAn[IllegalArgumentException]("Unknown message type")
    }
  }

  /** The consumer and producer APIs are not symmetric.  Convert a message sent to a producer into a message
    * received by a consumer.
    */
  implicit def transport(pmsg: ProducerMsg): ConsumerMsg = mockConsumerMessage(pmsg.key, pmsg.message)

  def mockConsumerMessage(key: Array[Byte], msg: Array[Byte]): ConsumerMsg = {
    val cmsg = mock[ConsumerMsg]
    cmsg.key() returns key
    cmsg.message() returns msg
    cmsg
  }

  def equalSF(expected: SimpleFeature): Matcher[SimpleFeature] = {
    sf: SimpleFeature => {
      sf.getID mustEqual expected.getID
      sf.getDefaultGeometry mustEqual expected.getDefaultGeometry
      sf.getAttributes mustEqual expected.getAttributes
      sf.getUserData mustEqual expected.getUserData
    }
  }
}
