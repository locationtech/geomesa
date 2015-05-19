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

import com.google.common.base.Ticker
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LiveFeatureCacheTest extends Specification with Mockito with SimpleFeatureMatchers {

  import KafkaConsumerTestData._

  "LiveFeatureCache" should {

    sequential

    val lfc = new LiveFeatureCache(sft, None)(Ticker.systemTicker())

    "handle a CreateOrUpdate message" >> {

      val msg = CreateOrUpdate(new Instant(1000), track0v0)

      lfc.createOrUpdateFeature(msg)

      lfc.cache.size() mustEqual 1
      lfc.cache.getIfPresent("track0") must equalFeatureHolder(track0v0)

      lfc.features must haveSize(1)
      lfc.features.get("track0") must beSome(featureHolder(track0v0))

      lfc.qt.size() mustEqual 1
      lfc.qt.queryAll() must containFeatureHolders(track0v0)
    }

    "and a second CreateOrUpdate message" >> {

      val msg = CreateOrUpdate(new Instant(2000), track1v0)

      lfc.createOrUpdateFeature(msg)

      lfc.cache.size() mustEqual 2
      lfc.cache.getIfPresent("track1") must equalFeatureHolder(track1v0)

      lfc.features must haveSize(2)
      lfc.features.get("track1") must beSome(featureHolder(track1v0))

      lfc.qt.size() mustEqual 2
      lfc.qt.queryAll() must containFeatureHolders(track0v0, track1v0)
    }

    "and use the most recent version of a feature" >> {

      val msg = CreateOrUpdate(new Instant(3000), track0v1)

      lfc.createOrUpdateFeature(msg)

      lfc.cache.size() mustEqual 2
      lfc.cache.getIfPresent("track0") must equalFeatureHolder(track0v1)

      lfc.features must haveSize(2)
      lfc.features.get("track0") must beSome(featureHolder(track0v1))

      lfc.qt.size() mustEqual 2
      lfc.qt.queryAll() must containFeatureHolders(track0v1, track1v0)
    }

    "and handle a Delete message" >> {

      val msg = Delete(new Instant(4000), "track0")

      lfc.removeFeature(msg)

      lfc.cache.size() mustEqual 1
      lfc.cache.getIfPresent("track0") must beNull

      lfc.features must haveSize(1)
      lfc.features.get("track0") must beNone

      lfc.qt.size() mustEqual 1
      lfc.qt.queryAll() must containFeatureHolders(track1v0)
    }

    "and a more CreateOrUpdate message" >> {

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5000), track2v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5005), track1v2))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5010), track3v0))

      lfc.cache.size() mustEqual 3

      lfc.features must haveSize(3)
      lfc.features.get("track1") must beSome(featureHolder(track1v2))
      lfc.features.get("track2") must beSome(featureHolder(track2v0))
      lfc.features.get("track3") must beSome(featureHolder(track3v0))

      lfc.qt.size() mustEqual 3
      lfc.qt.queryAll() must containFeatureHolders(track1v2, track2v0, track3v0)
    }

    "and handle a Clear message" >> {

      lfc.clear()

      lfc.cache.size() mustEqual 0
      lfc.features must haveSize(0)
      lfc.qt.size() mustEqual 0
    }
  }

  "LiveFeatureCache with expiry" should {

    sequential

    implicit val ticker = new MockTicker
    ticker.tic = 1000000L // ns

    val lfc = new LiveFeatureCache(sft, Some(5L)) // ms

    "handle a CreateOrUpdate message" >> {

      val msg = CreateOrUpdate(new Instant(1000), track0v0)
      lfc.createOrUpdateFeature(msg)

      ticker.tic = 2000000L // ns

      lfc.cache.size() mustEqual 1
      lfc.cache.getIfPresent("track0") must equalFeatureHolder(track0v0)

      lfc.features must haveSize(1)
      lfc.features.get("track0") must beSome(featureHolder(track0v0))

      lfc.qt.size() mustEqual 1
      lfc.qt.queryAll() must containFeatureHolders(track0v0)
    }

    "and expire message correctly" >> {

      ticker.tic = 7000000L
      lfc.cache.cleanUp()

      lfc.cache.size() mustEqual 0
      lfc.cache.getIfPresent("track0") must beNull

      lfc.features must haveSize(0)
      lfc.features.get("track0") must beNone

      lfc.qt.size() mustEqual 0
    }
  }
}

class MockTicker extends Ticker {

  var tic: Long = 0L

  def read(): Long = tic
}
