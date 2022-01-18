/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import com.github.benmanes.caffeine.cache.Ticker
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.ExpirationMocking.{MockTicker, ScheduledExpiry, WrappedRunnable}
import org.locationtech.geomesa.kafka.data.KafkaDataStore.{IndexConfig, IndexResolution, IngestTimeConfig, NeverExpireConfig}
import org.locationtech.geomesa.kafka.data.{KafkaDataStore, KafkaDataStoreFactory, KafkaDataStoreParams}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.mockito.ArgumentMatchers
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Collections
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

@RunWith(classOf[JUnitRunner])
class KafkaFeatureCacheViewTest extends Specification with Mockito {

  import scala.concurrent.duration._

  sequential

  val wholeWorldFilter = ECQL.toFilter("INTERSECTS(geom, POLYGON((-180 -90, -180 90, 0 90, 180 90, 180 -90, 0 -90, -180 -90)))")

  val sft = SimpleFeatureTypes.createType("track", "trackId:String,*geom:Point:srid=4326")

  val views = {
    val config =
    """{
      |  track = [
      |    { type-name = track2, filter = "bbox(geom,49,19,51,22)", transform = [ "geom" ] }
      |    { type-name = track3, transform = [ "geom" ] }
      |    { type-name = track4, filter = "bbox(geom,49,19,51,22)" }
      |  ]
      |}""".stripMargin
    val view =
      KafkaDataStoreFactory.parseLayerViewConfig(Collections.singletonMap(KafkaDataStoreParams.LayerViews.key, config))
    view("track").map(KafkaDataStore.createLayerView(sft, _))
  }

  val res = IndexResolution(360, 180)

  val track0v0 = track("track0", "POINT (30 30)")
  val track0v1 = track("track0", "POINT (35 30)")

  val track1v0 = track("track1", "POINT (50 20)")
  val track1v1 = track("track1", "POINT (50 21)")

  val track2v0 = track("track2", "POINT (30 30)")

  val track3v0 = track("track3", "POINT (0 60)")

  def track(id: String, track: String): SimpleFeature = ScalaSimpleFeature.create(sft, id, id, track)

  def createCache(expiry: Option[(Duration, ScheduledExecutorService, Ticker)] = None): KafkaFeatureCache = {
    val config = expiry match {
      case None => IndexConfig(NeverExpireConfig, res, Seq.empty, Seq.empty, lazyDeserialization = true, None)
      case Some((e, es, t)) => IndexConfig(IngestTimeConfig(e), res, Seq.empty, Seq.empty, lazyDeserialization = true, Some((es, t)))
    }
    KafkaFeatureCache(sft, config, views)
  }

  "KafkaFeatureCacheView" should {

    "put" >> {
      val cache = createCache()
      try {
        cache.put(track0v0)
        cache.put(track1v0)

        cache.query(track0v0.getID) must beSome(track0v0)
        cache.query(track1v0.getID) must beSome(track1v0)
        cache.query(wholeWorldFilter).toSeq must containTheSameElementsAs(Seq(track0v0, track1v0))
        cache.size() mustEqual 2

        val Seq(filteredTransformed, transformed, filtered) = cache.views

        filteredTransformed.query(track0v0.getID) must beNone
        filteredTransformed.query(track1v0.getID) must
          beSome(ScalaSimpleFeature.retype(filteredTransformed.sft, track1v0))
        filteredTransformed.query(wholeWorldFilter).toSeq must
          containTheSameElementsAs(Seq(ScalaSimpleFeature.retype(filteredTransformed.sft, track1v0)))
        filteredTransformed.size() mustEqual 1

        transformed.query(track0v0.getID) must beSome(ScalaSimpleFeature.retype(transformed.sft, track0v0))
        transformed.query(track1v0.getID) must beSome(ScalaSimpleFeature.retype(transformed.sft, track1v0))
        transformed.query(wholeWorldFilter).toSeq must
          containTheSameElementsAs(Seq(track0v0, track1v0).map(ScalaSimpleFeature.retype(transformed.sft, _)))
        transformed.size() mustEqual 2

        filtered.query(track0v0.getID) must beNone
        filtered.query(track1v0.getID) must beSome(ScalaSimpleFeature.retype(filtered.sft, track1v0))
        filtered.query(wholeWorldFilter).toSeq must
          containTheSameElementsAs(Seq(ScalaSimpleFeature.retype(filtered.sft, track1v0)))
        filtered.size() mustEqual 1
      } finally {
        cache.close()
      }
    }

    "update" >> {
      val cache = createCache()
      try {
        cache.put(track0v0)
        cache.put(track1v0)
        cache.put(track0v1)
        cache.put(track1v1)

        cache.query(track0v1.getID) must beSome(track0v1)
        cache.query(track1v1.getID) must beSome(track1v1)
        cache.query(wholeWorldFilter).toSeq must containTheSameElementsAs(Seq(track0v1, track1v1))
        cache.size() mustEqual 2

        val Seq(filteredTransformed, transformed, filtered) = cache.views

        filteredTransformed.query(track0v1.getID) must beNone
        filteredTransformed.query(track1v1.getID) must
          beSome(ScalaSimpleFeature.retype(filteredTransformed.sft, track1v1))
        filteredTransformed.query(wholeWorldFilter).toSeq must
          containTheSameElementsAs(Seq(ScalaSimpleFeature.retype(filteredTransformed.sft, track1v1)))
        filteredTransformed.size() mustEqual 1

        transformed.query(track0v1.getID) must beSome(ScalaSimpleFeature.retype(transformed.sft, track0v1))
        transformed.query(track1v1.getID) must beSome(ScalaSimpleFeature.retype(transformed.sft, track1v1))
        transformed.query(wholeWorldFilter).toSeq must
          containTheSameElementsAs(Seq(track0v1, track1v1).map(ScalaSimpleFeature.retype(transformed.sft, _)))
        transformed.size() mustEqual 2

        filtered.query(track0v1.getID) must beNone
        filtered.query(track1v1.getID) must beSome(ScalaSimpleFeature.retype(filtered.sft, track1v1))
        filtered.query(wholeWorldFilter).toSeq must
          containTheSameElementsAs(Seq(ScalaSimpleFeature.retype(filtered.sft, track1v1)))
        filtered.size() mustEqual 1
      } finally {
        cache.close()
      }
    }

    "remove" >> {
      val cache = createCache()
      try {
        cache.put(track0v0)
        cache.put(track1v0)
        cache.put(track1v1)
        cache.remove(track1v1.getID)

        cache.query(track0v1.getID) must beSome(track0v0)
        cache.query(track1v1.getID) must beNone
        cache.query(wholeWorldFilter).toSeq mustEqual Seq(track0v0)
        cache.size() mustEqual 1

        val Seq(filteredTransformed, transformed, filtered) = cache.views

        filteredTransformed.query(track0v1.getID) must beNone
        filteredTransformed.query(track1v1.getID) must beNone
        filteredTransformed.query(wholeWorldFilter).toSeq must beEmpty
        filteredTransformed.size() mustEqual 0

        transformed.query(track0v1.getID) must beSome(ScalaSimpleFeature.retype(transformed.sft, track0v0))
        transformed.query(track1v1.getID) must beNone
        transformed.query(wholeWorldFilter).toSeq mustEqual Seq(ScalaSimpleFeature.retype(transformed.sft, track0v0))
        transformed.size() mustEqual 1

        filtered.query(track0v1.getID) must beNone
        filtered.query(track1v0.getID) must beNone
        filtered.query(wholeWorldFilter).toSeq must beEmpty
        filtered.size() mustEqual 0
      } finally {
        cache.close()
      }
    }

    "clear" >> {
      val cache = createCache()
      try {
        cache.put(track0v0)
        cache.put(track1v0)
        cache.put(track0v1)
        cache.remove(track0v1.getID)
        cache.put(track2v0)
        cache.put(track1v1)
        cache.put(track3v0)
        cache.clear()
        foreach(Seq(cache) ++ cache.views) { cache =>
          cache.query(wholeWorldFilter).toSeq must beEmpty
          cache.size() mustEqual 0
        }
      } finally {
        cache.close()
      }
    }

    "expire" >> {
      val ex = mock[ScheduledExecutorService]
      val ticker = new MockTicker()
      val cache = createCache(Some((Duration("100ms"), ex, ticker)))

      try {
        val expire = new WrappedRunnable(100L)
        ex.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
          expire.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
          new ScheduledExpiry(expire)
        }

        cache.put(track0v0)

        expire.runnable must not(beNull)
        there was one(ex).schedule(ArgumentMatchers.eq(expire.runnable), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

        cache.size() mustEqual 1
        cache.query(track0v0.getID) must beSome(track0v0)
        cache.query(wholeWorldFilter).toSeq mustEqual Seq(track0v0)

        val Seq(filteredTransformed, transformed, filtered) = cache.views

        filteredTransformed.size() mustEqual 0
        filteredTransformed.query(track0v0.getID) must beNone
        filteredTransformed.query(wholeWorldFilter).toSeq must beEmpty

        transformed.size() mustEqual 1
        transformed.query(track0v0.getID) must beSome(ScalaSimpleFeature.retype(transformed.sft, track0v0))
        transformed.query(wholeWorldFilter).toSeq mustEqual Seq(ScalaSimpleFeature.retype(transformed.sft, track0v0))

        filtered.size() mustEqual 0
        filtered.query(track0v0.getID) must beNone
        filtered.query(wholeWorldFilter).toSeq must beEmpty

        // move time forward and run the expiration
        ticker.millis += 100L
        expire.runnable.run()

        foreach(Seq(cache, filteredTransformed, transformed, filtered)) { cache =>
          cache.query(track0v0.getID) must beNone
          cache.query(wholeWorldFilter).toSeq must beEmpty
          cache.size() mustEqual 0
        }
      } finally {
        cache.close()
      }
    }
  }
}

