/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.kafka

import com.google.common.base.Ticker
import com.vividsolutions.jts.geom.Envelope
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class LiveFeatureCacheTest extends Specification with Mockito with SimpleFeatureMatchers {

  import KafkaConsumerTestData._

  implicit val ticker = Ticker.systemTicker()
  val wholeWorld = new Envelope(-180, 180, -90, 90)

  "LiveFeatureCache" should {

    "handle a CreateOrUpdate message" >> {
      val lfc = new LiveFeatureCache(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))

      lfc.cache.size() mustEqual 1
      lfc.cache.getIfPresent("track0") must equalFeatureHolder(track0v0)

      lfc.features must haveSize(1)
      lfc.features.get("track0") must beSome(featureHolder(track0v0))

      lfc.spatialIndex.query(wholeWorld).toList.asJava must containTheSameFeatureHoldersAs(track0v0)
    }

    "handle two CreateOrUpdate messages" >> {
      val lfc = new LiveFeatureCache(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))

      lfc.cache.size() mustEqual 2
      lfc.cache.getIfPresent("track1") must equalFeatureHolder(track1v0)

      lfc.features must haveSize(2)
      lfc.features.get("track1") must beSome(featureHolder(track1v0))

      lfc.spatialIndex.query(wholeWorld).toList.asJava must containTheSameFeatureHoldersAs(track0v0, track1v0)
    }

    "use the most recent version of a feature" >> {
      val lfc = new LiveFeatureCache(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(3000), track0v1))

      lfc.cache.size() mustEqual 2
      lfc.cache.getIfPresent("track0") must equalFeatureHolder(track0v1)

      lfc.features must haveSize(2)
      lfc.features.get("track0") must beSome(featureHolder(track0v1))

      lfc.spatialIndex.query(wholeWorld).toList.asJava must containTheSameFeatureHoldersAs(track0v1, track1v0)
    }

    "handle a Delete message" >> {
      val lfc = new LiveFeatureCache(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(3000), track0v1))
      lfc.removeFeature(Delete(new Instant(4000), "track0"))

      lfc.cache.size() mustEqual 1
      lfc.cache.getIfPresent("track0") must beNull

      lfc.features must haveSize(1)
      lfc.features.get("track0") must beNone

      lfc.spatialIndex.query(wholeWorld).toList.asJava must containTheSameFeatureHoldersAs(track1v0)
    }

    "handle a Clear message" >> {
      val lfc = new LiveFeatureCache(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(3000), track0v1))
      lfc.removeFeature(Delete(new Instant(4000), "track0"))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5000), track2v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5005), track1v2))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5010), track3v0))

      lfc.clear()

      lfc.cache.size() mustEqual 0
      lfc.features must haveSize(0)
      lfc.spatialIndex.query(wholeWorld) must beEmpty
    }
  }

  "LiveFeatureCache with expiry" should {

    "handle a CreateOrUpdate message" >> {
      implicit val ticker = new MockTicker
      ticker.tic = 1000000L // ns

      val lfc = new LiveFeatureCache(sft, Some(5L)) // ms
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))

      ticker.tic = 2000000L // ns

      lfc.cache.size() mustEqual 1
      lfc.cache.getIfPresent("track0") must equalFeatureHolder(track0v0)

      lfc.features must haveSize(1)
      lfc.features.get("track0") must beSome(featureHolder(track0v0))

      lfc.spatialIndex.query(wholeWorld).toList.asJava must containTheSameFeatureHoldersAs(track0v0)
    }

    "expire message correctly" >> {
      implicit val ticker = new MockTicker
      ticker.tic = 1000000L // ns

      val lfc = new LiveFeatureCache(sft, Some(5L)) // ms
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))

      ticker.tic = 7000000L
      lfc.cache.cleanUp()

      lfc.cache.size() mustEqual 0
      lfc.cache.getIfPresent("track0") must beNull

      lfc.features must haveSize(0)
      lfc.features.get("track0") must beNone

      lfc.spatialIndex.query(wholeWorld) must beEmpty
    }
  }
}

class MockTicker extends Ticker {

  var tic: Long = 0L

  def read(): Long = tic
}
