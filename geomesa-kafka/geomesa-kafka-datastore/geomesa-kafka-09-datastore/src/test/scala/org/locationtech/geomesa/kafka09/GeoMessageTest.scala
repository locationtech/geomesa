/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka09

import java.nio.charset.StandardCharsets

import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.kafka.{CreateOrUpdate, GeoMessage}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.Matcher
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class GeoMessageTest extends Specification with Mockito {

  sequential

  type ProducerMsg = KafkaGeoMessageEncoder#MSG
  type ConsumerMsg = KafkaGeoMessageDecoder#MSG
  
  val topic = "test_topic"
  val schema = SimpleFeatureTypes.createType("KafkaGeoMessageTest", "name:String,*geom:Point:srid=4326")

  def createSimpleFeature: SimpleFeature = {
    val id = "test_id"
    val sfVals = List[AnyRef]("foo", WKTUtils.read("POINT(1 -1)"))
    new SimpleFeatureImpl(sfVals.asJava, schema, new FeatureIdImpl(id))
  }

  def encodeSF(sf: SimpleFeature): Array[Byte] = {
    val sfEncoder = new KryoFeatureSerializer(schema, SerializationOptions.withUserData)
    sfEncoder.serialize(sf)
  }

  "Clear" should {

    val msg = GeoMessage.clear()

    "be able to be encoded and decoded" >> {
      val encoder = new KafkaGeoMessageEncoder(schema)
      val encoded: ProducerMsg = encoder.encodeMessage(topic, msg)

      encoded must not(beNull)
      encoded.key must not(beNull)
      encoded.key.length mustEqual 10
      encoded.key(0) mustEqual 1
      encoded.key(1) mustEqual 'X'
      encoded.message mustEqual Array.empty[Byte]

      val decoder = new KafkaGeoMessageDecoder(schema)
      val decoded: GeoMessage = decoder.decode(encoded)

      decoded mustEqual msg
    }
  }

  "Delete" should {

    val id = "test_id"
    val msg = GeoMessage.delete(id)

    "be able to be encoded and decoded" >> {
      val encoder = new KafkaGeoMessageEncoder(schema)
      val encoded: ProducerMsg = encoder.encodeMessage(topic, msg)

      encoded must not(beNull)
      encoded.key must not(beNull)
      encoded.key.length mustEqual 10
      encoded.key(0) mustEqual 1
      encoded.key(1) mustEqual 'D'
      encoded.message mustEqual id.getBytes(StandardCharsets.UTF_8)

      val decoder = new KafkaGeoMessageDecoder(schema)
      val decoded: GeoMessage = decoder.decode(encoded)

      decoded mustEqual msg
    }
  }

  "CreateOrUpdate" should {

    val sf = createSimpleFeature
    val msg = GeoMessage.createOrUpdate(sf)

    "be able to be encoded and decoded" >> {
      val encoder = new KafkaGeoMessageEncoder(schema)
      val encoded: ProducerMsg = encoder.encodeMessage(topic, msg)

      encoded must not(beNull)
      encoded.key must not(beNull)
      encoded.key.length mustEqual 10
      encoded.key(0) mustEqual 1
      encoded.key(1) mustEqual 'C'
      encoded.message mustEqual encodeSF(sf)

      val decoder = new KafkaGeoMessageDecoder(schema)
      val decoded: GeoMessage = decoder.decode(encoded)

      decoded.isInstanceOf[CreateOrUpdate] must beTrue

      val cu = decoded.asInstanceOf[CreateOrUpdate]
      cu.feature must equalSF(sf)
    }
  }

  "KafkaGeoMessageDecoder" should {

    "throw an exception if the message key is null" >> {
      val decoder = new KafkaGeoMessageDecoder(schema)

      val msg = mockConsumerMessage(
        null,
        "garbage".getBytes(StandardCharsets.UTF_8))

      decoder.decode(msg) must throwAn[IllegalArgumentException]("Invalid message key: null")
    }

    "throw an exception if the message key length is incorrect" >> {
      val decoder = new KafkaGeoMessageDecoder(schema)

      val msg = mockConsumerMessage(
        Array[Byte](1, 1, 1),
        "garbage".getBytes(StandardCharsets.UTF_8))

      decoder.decode(msg) must throwAn[IllegalArgumentException]("Expecting 10 bytes")
    }

    "throw an exception if the version number is incorrect" >> {
      val decoder = new KafkaGeoMessageDecoder(schema)

      val msg = mockConsumerMessage(
        Array[Byte](0, 'X', 1, 2, 3, 4, 5, 6, 7, 8),
        "garbage".getBytes(StandardCharsets.UTF_8))

      decoder.decode(msg) must throwAn[IllegalArgumentException]("Unknown serialization version")
    }

    "throw an exception if the message type is invalid" >> {
      val decoder = new KafkaGeoMessageDecoder(schema)

      val msg = mockConsumerMessage(
        Array[Byte](1, 'Z', 0, 0, 0, 0, 0, 0, 0, 0),
        "garbage".getBytes(StandardCharsets.UTF_8))

      decoder.decode(msg) must throwAn[IllegalArgumentException]("Unknown message type")
    }

    "throw an exception if the message cannot be decoded" >> {
      val decoder = new KafkaGeoMessageDecoder(schema)

      val msg = mockConsumerMessage(
        Array[Byte](0, 'C', 1, 2, 3, 4, 5, 6, 7, 8),
        "garbage".getBytes(StandardCharsets.UTF_8))

      decoder.decode(msg) must throwAn[IllegalArgumentException]
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
