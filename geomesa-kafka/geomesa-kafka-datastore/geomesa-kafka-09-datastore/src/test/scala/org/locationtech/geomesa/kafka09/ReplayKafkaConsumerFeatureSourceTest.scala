/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka09

import org.geotools.data.EmptyFeatureReader
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka._
import org.locationtech.geomesa.utils.geotools.FR
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.matcher.{MatchResult, ValueCheck}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.annotation.tailrec

@RunWith(classOf[JUnitRunner])
class ReplayKafkaConsumerFeatureSourceTest extends Specification with Mockito with SimpleFeatureMatchers {

  import KafkaConsumerTestData._

  val topic = "testTopic"

  "feature source" should {

    "read messages from kafka" >> {

      "when bounds are not on messages" >> {

        val msgs = Seq(
          CreateOrUpdate(new Instant(10993), track0v0), // 0
          CreateOrUpdate(new Instant(11001), track3v0), // 1
          CreateOrUpdate(new Instant(11549), track3v1), // 2

          CreateOrUpdate(new Instant(11994), track0v1), // 3
          CreateOrUpdate(new Instant(11995), track1v0), // 4
          CreateOrUpdate(new Instant(11995), track3v2), // 5

          CreateOrUpdate(new Instant(12998), track1v1), // 6
          CreateOrUpdate(new Instant(13000), track2v0), // 7
          CreateOrUpdate(new Instant(13002), track3v3), // 8
          CreateOrUpdate(new Instant(13002), track0v2)) // 9

        val replayConfig = ReplayConfig(12000, 12000L, 100L)
        val fs = featureSource(msgs, replayConfig)

        val expected = msgs.slice(3, 6).reverse

        fs.messages.toSeq must containGeoMessages(expected)
      }

      "when bounds are on messages" >> {

        val msgs = Seq(
          CreateOrUpdate(new Instant(11549), track3v1), // 0
          CreateOrUpdate(new Instant(11994), track0v1), // 1

          CreateOrUpdate(new Instant(11995), track1v0), // 2
          CreateOrUpdate(new Instant(11995), track3v2), // 3
          CreateOrUpdate(new Instant(12998), track1v1), // 4
          CreateOrUpdate(new Instant(13000), track2v0), // 5
          CreateOrUpdate(new Instant(13002), track3v3), // 6
          CreateOrUpdate(new Instant(13002), track0v2), // 7

          CreateOrUpdate(new Instant(13444), track1v2), // 8
          CreateOrUpdate(new Instant(13996), track2v1)) // 9

        val replayConfig = ReplayConfig(12002L, 13002L, 7L)
        val fs = featureSource(msgs, replayConfig)

        val expected = msgs.slice(2, 8).reverse

        fs.messages.toSeq must containGeoMessages(expected)
      }

      "when end time is after last message" >> {

        val msgs = Seq(
          CreateOrUpdate(new Instant(11549), track3v1), // 0

          CreateOrUpdate(new Instant(11994), track0v1), // 1
          CreateOrUpdate(new Instant(11995), track1v0), // 2
          CreateOrUpdate(new Instant(11995), track3v2), // 3
          CreateOrUpdate(new Instant(12998), track1v1), // 4
          CreateOrUpdate(new Instant(13000), track2v0), // 5
          CreateOrUpdate(new Instant(13002), track3v3), // 6
          CreateOrUpdate(new Instant(13002), track0v2)) // 7

        val replayConfig = ReplayConfig(12000L, 14000L, 100L)
        val fs = featureSource(msgs, replayConfig)

        val expected = msgs.slice(1, 8).reverse

        fs.messages.toSeq must containGeoMessages(expected)
      }
    }

    "find message index by time" >> {

      val msgs = Seq(
        CreateOrUpdate(new Instant(12998), track1v1),   // 4
        CreateOrUpdate(new Instant(13000), track2v0),   // 3
        CreateOrUpdate(new Instant(13002), track3v3),   // 2
        CreateOrUpdate(new Instant(13002), track0v2),   // 1
        CreateOrUpdate(new Instant(13444), track1v2)    // 0
      )

      val replayConfig = ReplayConfig(12500L, 13500L, 0L)

      // lazy to prevent error from crashing test framework
      lazy val fs = featureSource(msgs, replayConfig)

      "when time is in window" >> {
        fs.indexAtTime(12998) must beSome(4)
        fs.indexAtTime(12999) must beSome(4)
        fs.indexAtTime(13000) must beSome(3)
        fs.indexAtTime(13001) must beSome(3)
        fs.indexAtTime(13002) must beSome(1)
        fs.indexAtTime(13003) must beSome(1)

        fs.indexAtTime(13443) must beSome(1)
        fs.indexAtTime(13444) must beSome(0)
        fs.indexAtTime(13445) must beSome(0)

        fs.indexAtTime(13500) must beSome(0)
      }

      "unless time is before first message in window" >> {
        fs.indexAtTime(12500) must beNone
        fs.indexAtTime(12997) must beNone
      }

      "or if time is outside window" >> {
        fs.indexAtTime(12490) must beNone
        fs.indexAtTime(12499) must beNone
        fs.indexAtTime(12501) must beNone
        fs.indexAtTime(12510) must beNone
      }
    }

    "create a snapshot" >> {

      val msgs = Seq(
        CreateOrUpdate(new Instant(10993), track0v0), // 0
        CreateOrUpdate(new Instant(11001), track3v0), // 1
        CreateOrUpdate(new Instant(11549), track3v1), // 2

        CreateOrUpdate(new Instant(11994), track0v1), // 3
        CreateOrUpdate(new Instant(11995), track1v0), // 4
        CreateOrUpdate(new Instant(11995), track3v2), // 5

        CreateOrUpdate(new Instant(12998), track1v1), // 6
        CreateOrUpdate(new Instant(13000), track2v0), // 7
        CreateOrUpdate(new Instant(13002), track3v3), // 8
        CreateOrUpdate(new Instant(13002), track0v2)) // 9

      val replayConfig = ReplayConfig(10000L, 13100L, 300L)
      lazy val replayType = KafkaDataStoreHelper.createReplaySFT(sft, replayConfig)
      lazy val fs = featureSource(msgs, replayType)

      def equalsSnapshot(expectedMsgs: Seq[GeoMessage], replayTime: Long): ValueCheck[ReplaySnapshotFeatureCache] = {
        s: ReplaySnapshotFeatureCache =>
          s.sft mustEqual replayType
          s.replayTime mustEqual replayTime
          s.events must equalGeoMessages(expectedMsgs)
      }

      "using a given valid time" >> {
        val expected = msgs.slice(3, 6).reverse
        val result = fs.snapshot(Some(12000L))
        result must beSome(equalsSnapshot(expected, 12000L))
      }

      "using the most recent time if none is given" >> {
        val expected = msgs.slice(6, 10).reverse
        val result = fs.snapshot(None)
        result must beSome(equalsSnapshot(expected, 13100L))
      }

      "or not if time is invalid" >> {
        val result = fs.snapshot(Some(20000L))
        result must beNone
      }

      "or not if no data is available" >> {
        val result = fs.snapshot(Some(11900L))
        result must beNone
      }
    }

    "get a reader containing the correct features" >> {
      val msgs = Seq(
        CreateOrUpdate(new Instant(10993), track0v0), // 0
        CreateOrUpdate(new Instant(11001), track3v0), // 1
        CreateOrUpdate(new Instant(11549), track3v1), // 2

        CreateOrUpdate(new Instant(11994), track0v1), // 3
        CreateOrUpdate(new Instant(11995), track1v0), // 4
        CreateOrUpdate(new Instant(11995), track3v2), // 5

        CreateOrUpdate(new Instant(12998), track1v1), // 6
        CreateOrUpdate(new Instant(13000), track2v0), // 7
        CreateOrUpdate(new Instant(13002), track3v3), // 8
        CreateOrUpdate(new Instant(13002), track0v2)) // 9

      val replayConfig = ReplayConfig(10000L, 12000L, 100L)
      val replayType = KafkaDataStoreHelper.createReplaySFT(sft, replayConfig)
      val fs = featureSource(msgs, replayType)

      val result = fs.getReaderForFilter(Filter.INCLUDE)
      validateFR(result, expect(replayType, 12000L, track0v1, track1v0, track3v2))
    }

    "or an empty reader if no data" >> {
      val replayConfig = ReplayConfig(12000, 12000L, 100L)
      val replayType = KafkaDataStoreHelper.createReplaySFT(sft, replayConfig)
      val fs = featureSource(Seq.empty[GeoMessage], replayType)

      val result = fs.getReaderForFilter(Filter.INCLUDE)
      result must beAnInstanceOf[EmptyFeatureReader[SimpleFeatureType, SimpleFeature]]
      result.getFeatureType mustEqual replayType
    }
  }

  def featureSource(messages: Seq[GeoMessage], replayConfig: ReplayConfig): ReplayKafkaConsumerFeatureSource = {
    val replayType = KafkaDataStoreHelper.createReplaySFT(sft, replayConfig)

    featureSource(messages, replayConfig, replayType)
  }

  def featureSource(messages: Seq[GeoMessage], replayType: SimpleFeatureType): ReplayKafkaConsumerFeatureSource = {
    val replayConfig = KafkaDataStoreHelper.extractReplayConfig(replayType).get

    featureSource(messages, replayConfig, replayType)
  }

  def featureSource(messages: Seq[GeoMessage], replayConfig: ReplayConfig, replayType: SimpleFeatureType): ReplayKafkaConsumerFeatureSource = {
    val mockKafka = new MockKafka
    val consumerFactory = mockKafka.kafkaConsumerFactory

    val entry = null

    val encoder = new KafkaGeoMessageEncoder(sft)
    messages.foreach(msg => mockKafka.send(encoder.encodeMessage(topic, msg)))

    new ReplayKafkaConsumerFeatureSource(entry, replayType, sft, topic, consumerFactory, replayConfig, null)
  }

  @tailrec
  final def validateFR(actual: FR, expected: Set[SimpleFeature]): MatchResult[Any] = {

    if (!actual.hasNext) {
      // exhausted - must be expecting none
      expected must beEmpty
    } else {
      val next = actual.next()

      expected.head.equals(next)
      val result = expected aka s"Unexpected value found: $next" must contain(next)
      if (!result.isSuccess) {
        result
      } else {
        validateFR(actual, expected - next)
      }
    }

  }
}
