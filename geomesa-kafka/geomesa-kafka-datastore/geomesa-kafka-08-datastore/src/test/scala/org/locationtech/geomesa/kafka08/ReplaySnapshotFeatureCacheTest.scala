/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka08

import com.vividsolutions.jts.geom.Envelope
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka._
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.Matcher
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReplaySnapshotFeatureCacheTest extends Specification with SimpleFeatureMatchers {

  import KafkaConsumerTestData._

  // the ReplayConfig here doesn't matter
  val replayType = KafkaDataStoreHelper.createReplaySFT(sft, ReplayConfig(0, 0, 0))

  // replay time is arbitrary for this test
  val replayTime = 1000L
  val wholeWorld = new Envelope(-180, 180, -90, 90)

  "ReplaySnapshotFeatureCache" should {

    "correctly process a create message" >> {
      val msgs = Seq(CreateOrUpdate(new Instant(1000L), track0v0))

      val cache = new ReplaySnapshotFeatureCache(replayType, replayTime, msgs)

      cache.features must haveSize(1)

      val holder = cache.features("track0")
      holder.sf must equalSFWithReplayTime(track0v0)

      cache.spatialIndex.query(wholeWorld) must haveSize(1)
      val queryResult = cache.spatialIndex.query(holder.env).toSeq
      queryResult must haveSize(1)
      queryResult.head must beAnInstanceOf[SimpleFeature]
      queryResult.head.asInstanceOf[SimpleFeature] must equalSFWithReplayTime(track0v0)
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

      cache.spatialIndex.query(wholeWorld) must haveSize(1)
      val queryResult = cache.spatialIndex.query(holder.env).toSeq
      queryResult must haveSize(1)
      queryResult.head must beAnInstanceOf[SimpleFeature]
      queryResult.head.asInstanceOf[SimpleFeature] must equalSFWithReplayTime(track0v3)
    }

    "exclude deleted features" >> {
      val msgs = Seq(
        Delete(new Instant(5000L), "track0"),
        CreateOrUpdate(new Instant(4000L), track0v2)
      )

      val cache = new ReplaySnapshotFeatureCache(replayType, replayTime, msgs)

      cache.features must haveSize(0)
      cache.features.get("track0") must beNone

      cache.spatialIndex.query(wholeWorld) must beEmpty
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

      cache.spatialIndex.query(wholeWorld) must haveSize(1)
      val queryResult = cache.spatialIndex.query(holder.env).toSeq
      queryResult must haveSize(1)
      queryResult.head must beAnInstanceOf[SimpleFeature]
      queryResult.head.asInstanceOf[SimpleFeature] must equalSFWithReplayTime(track0v3)
    }

    "not handle Clear messages" >> {
      val msgs = Seq(Clear(new Instant(1000L)))

      val cache = new ReplaySnapshotFeatureCache(replayType, replayTime, msgs)
      
      // features is populated lazily
      cache.features must throwA[IllegalStateException]
    }
  }

  def equalSFWithReplayTime(expected: SimpleFeature): Matcher[SimpleFeature] = {
    val expectedWithReplayTime = new ReplayTimeHelper(replayType, replayTime).reType(expected)
    equalSF(expectedWithReplayTime)
  }
}
