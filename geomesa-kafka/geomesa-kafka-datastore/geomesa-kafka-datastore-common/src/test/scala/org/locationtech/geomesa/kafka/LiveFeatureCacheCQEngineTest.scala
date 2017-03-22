/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import com.github.benmanes.caffeine.cache.Ticker
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureReader
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class LiveFeatureCacheCQEngineTest extends Specification with Mockito with SimpleFeatureMatchers {

  import KafkaConsumerTestData._

  implicit val ticker = Ticker.systemTicker()
  val wholeWorldFilter = ECQL.toFilter("INTERSECTS(geom, POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90)))")

  "LiveFeatureCache" should {

    "handle a CreateOrUpdate message" >> {
      val lfc = new LiveFeatureCacheCQEngine(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))

      lfc.size() mustEqual 1
      lfc.getFeatureById("track0") must equalFeatureHolder(track0v0)

      lfc.getReaderForFilter(wholeWorldFilter).toIterator.toList.asJava must containTheSameFeatureHoldersAs(track0v0)
    }

    "handle two CreateOrUpdate messages" >> {
      val lfc = new LiveFeatureCacheCQEngine(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))

      lfc.size() mustEqual 2
      lfc.getFeatureById("track1") must equalFeatureHolder(track1v0)

      lfc.getReaderForFilter(wholeWorldFilter).toIterator.toList.asJava must containTheSameFeatureHoldersAs(track0v0, track1v0)
    }

    "use the most recent version of a feature" >> {
      val lfc = new LiveFeatureCacheCQEngine(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(3000), track0v1))

      lfc.size() mustEqual 2
      lfc.getFeatureById("track0") must equalFeatureHolder(track0v1)

      lfc.getReaderForFilter(wholeWorldFilter).toIterator.toList.asJava must containTheSameFeatureHoldersAs(track0v1, track1v0)
    }

    "handle a Delete message" >> {
      val lfc = new LiveFeatureCacheCQEngine(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(3000), track0v1))
      lfc.removeFeature(Delete(new Instant(4000), "track0"))

      lfc.size() mustEqual 1
      lfc.getFeatureById("track0") must beNull

      lfc.getReaderForFilter(wholeWorldFilter).toIterator.toList.asJava must containTheSameFeatureHoldersAs(track1v0)

    }

    "handle a Clear message" >> {
      val lfc = new LiveFeatureCacheCQEngine(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(3000), track0v1))
      lfc.removeFeature(Delete(new Instant(4000), "track0"))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5000), track2v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5005), track1v2))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5010), track3v0))

      lfc.clear()

      lfc.size() mustEqual 0
      lfc.getReaderForFilter(wholeWorldFilter).hasNext mustEqual false
    }
  }


  "LiveFeatureCache with expiry" should {

    "handle a CreateOrUpdate message" >> {
      implicit val ticker = new MockTicker
      ticker.tic = 1000000L // ns

      val lfc = new LiveFeatureCacheCQEngine(sft, Some(5L)) // ms
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))

      ticker.tic = 2000000L // ns

      lfc.size() mustEqual 1
      lfc.getFeatureById("track0") must equalFeatureHolder(track0v0)

      lfc.getReaderForFilter(wholeWorldFilter).toIterator.toList.asJava must containTheSameFeatureHoldersAs(track0v0)
   }

// TODO figure out why this test is failing randomly...
// https://geomesa.atlassian.net/browse/GEOMESA-1488
//    "expire message correctly" >> {
//      implicit val ticker = new MockTicker
//      ticker.tic = 1000000L // ns
//
//      val lfc = new LiveFeatureCacheCQEngine(sft, Some(5L)) // ms
//      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
//
//      ticker.tic = 7000000L
//      lfc.cleanUp()
//
//      lfc.size() mustEqual 0
//      lfc.getFeatureById("track0") must beNull
//
//      lfc.getReaderForFilter(wholeWorldFilter).hasNext mustEqual false
//    }
  }
}

