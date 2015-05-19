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

import org.joda.time.Instant
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.Matcher
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReplaySnapshotFeatureCacheTest extends Specification with SimpleFeatureMatchers {

  import KafkaConsumerTestData._

  // the ReplayConfig here doesn't matter
  val replayType = KafkaDataStoreHelper.prepareForReplay(sft, ReplayConfig(0, 0, 0))

  // replay time is arbitary for this test
  val replayTime = 1000L

  "ReplaySnapshotFeatureCache" should {

    "correctly process a create message" >> {
      val msgs = Seq(CreateOrUpdate(new Instant(1000L), track0v0))

      val cache = new ReplaySnapshotFeatureCache(replayType, replayTime, msgs)

      cache.features must haveSize(1)

      val holder = cache.features("track0")
      holder.sf must equalSFWithReplayTime(track0v0)

      cache.qt.size() mustEqual 1
      val queryResult = cache.qt.query(holder.env)
      queryResult.size() mustEqual 1
      queryResult.get(0) must beAnInstanceOf[SimpleFeature]
      queryResult.get(0).asInstanceOf[SimpleFeature] must equalSFWithReplayTime(track0v0)
    }

    "use the most recent version of a feature" >> {
      val msgs = Seq(
        CreateOrUpdate(new Instant(5000L), track0v3),
        CreateOrUpdate(new Instant(4000L), track0v2),
        CreateOrUpdate(new Instant(3000L), track0v1),
        CreateOrUpdate(new Instant(2000L), track0v0))

      val cache = new ReplaySnapshotFeatureCache(replayType, replayTime, msgs)

      cache.features must haveSize(1)

      val holder = cache.features("track0")
      holder.sf must equalSFWithReplayTime(track0v3)

      cache.qt.size() mustEqual 1
      val queryResult = cache.qt.query(holder.env)
      queryResult.size() mustEqual 1
      queryResult.get(0) must beAnInstanceOf[SimpleFeature]
      queryResult.get(0).asInstanceOf[SimpleFeature] must equalSFWithReplayTime(track0v3)
    }

    "exclude deleted features" >> {
      val msgs = Seq(
        Delete(new Instant(5000L), "track0"),
        CreateOrUpdate(new Instant(4000L), track0v2)
      )

      val cache = new ReplaySnapshotFeatureCache(replayType, replayTime, msgs)

      cache.features must haveSize(0)
      cache.features.get("track0") must beNone

      cache.qt.size() mustEqual 0
    }

    "include features created after a delete" >> {
      val msgs = Seq(
        CreateOrUpdate(new Instant(6000L), track0v3),
        Delete(new Instant(5000L), "track0"),
        CreateOrUpdate(new Instant(4000L), track0v2)
      )

      val cache = new ReplaySnapshotFeatureCache(replayType, replayTime, msgs)

      cache.features must haveSize(1)

      val holder = cache.features("track0")
      holder.sf must equalSFWithReplayTime(track0v3)

      cache.qt.size() mustEqual 1
      val queryResult = cache.qt.query(holder.env)
      queryResult.size() mustEqual 1
      queryResult.get(0) must beAnInstanceOf[SimpleFeature]
      queryResult.get(0).asInstanceOf[SimpleFeature] must equalSFWithReplayTime(track0v3)
    }

    "not handle Clear messages" >> {
      val msgs = Seq(Clear(new Instant(1000L)))

      val cache = new ReplaySnapshotFeatureCache(sft, replayTime, msgs)
      
      // features is populated lazily
      cache.features must throwA[IllegalStateException]
    }
  }

  def equalSFWithReplayTime(expected: SimpleFeature): Matcher[SimpleFeature] = {
    val expectedWithReplayTime = new ReplayTimeHelper(replayType, replayTime).addReplayTime(expected)
    equalSF(expectedWithReplayTime)
  }
}
