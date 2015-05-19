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

import org.geotools.data.{Transaction, EmptyFeatureReader}
import org.geotools.data.store.{ContentDataStore, ContentState, ContentEntry}
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.FR
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
      lazy val replayType = KafkaDataStoreHelper.prepareForReplay(sft, replayConfig)
      lazy val fs = featureSource(msgs, replayConfig)

      def equalsSnapshot(expectedMsgs: Seq[GeoMessage], replayTime: Long): ValueCheck[ReplaySnapshotFeatureCache] = {
        val helper = new ReplayTimeHelper(replayType, replayTime)

        val expectedWithTime = expectedMsgs.map {
          case CreateOrUpdate(ts, sf) => CreateOrUpdate(ts, helper.addReplayTime(sf))
          case a => a
        }

        s: ReplaySnapshotFeatureCache =>
          s.schema mustEqual sft
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

      val replayConfig = ReplayConfig(10000, 12000L, 100L)
      val fs = featureSource(msgs, replayConfig)

      val result = fs.getReaderForFilter(Filter.INCLUDE)
      validateFR(result, expect(track0v1, track1v0, track3v2))
    }

    "or an empty reader if no data" >> {
      val replayConfig = ReplayConfig(12000, 12000L, 100L)
      val fs = featureSource(Seq.empty[GeoMessage], replayConfig)

      val result = fs.getReaderForFilter(Filter.INCLUDE)
      result must beAnInstanceOf[EmptyFeatureReader[SimpleFeatureType, SimpleFeature]]
      result.getFeatureType mustEqual sft
    }
  }

  def featureSource(messages: Seq[GeoMessage], replayConfig: ReplayConfig): ReplayKafkaConsumerFeatureSource = {
    val replayType = KafkaDataStoreHelper.prepareForReplay(sft, replayConfig)

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

    new ReplayKafkaConsumerFeatureSource(entry, replayType, sft, topic, consumerFactory, replayConfig)
  }

  @tailrec
  final def validateFR(actual: FR, expected: Set[SimpleFeature]): MatchResult[Any] = {

    if (!actual.hasNext) {
      // exhausted - must be expecting none
      expected must beEmpty
    } else {
      val next = actual.next()

      val result = expected aka s"Unexpected value found: $next" must contain(next)
      if (!result.isSuccess) {
        result
      } else {
        validateFR(actual, expected - next)
      }
    }

  }
}
