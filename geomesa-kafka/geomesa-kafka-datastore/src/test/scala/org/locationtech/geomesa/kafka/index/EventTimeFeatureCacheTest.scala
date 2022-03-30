/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.util.Date
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.ExpirationMocking.{MockTicker, ScheduledExpiry, WrappedRunnable}
import org.locationtech.geomesa.kafka.data.KafkaDataStore._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.ArgumentMatchers
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class EventTimeFeatureCacheTest extends Specification with Mockito {

  sequential // sequential helps expiration timing to be more consistent

  val sft = SimpleFeatureTypes.createType("track", "trackId:String,dtg:Date:default=true,*geom:Point:srid=4326")

  val res = IndexResolution(180, 90)

  "EventTimeFeatureCache" should {
    "order by event time" in {
      val ex = EventTimeConfig(Duration.Inf, "dtg", ordered = true)
      val config = IndexConfig(ex, res, Seq.empty, Seq.empty, lazyDeserialization = true, None)

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        val sf1 = ScalaSimpleFeature.create(sft, "1", "first", "2018-01-01T12:00:00.000Z", "POINT (-78.0 35.0)")
        cache.put(sf1)

        val sf2 = ScalaSimpleFeature.create(sft, "1", "second", "2018-01-01T11:59:55.000Z", "POINT (-78.0 35.0)")
        cache.put(sf2)

        cache.query("1") must beSome(sf1.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf1)

        val sf3 = ScalaSimpleFeature.create(sft, "1", "third", "2018-01-01T12:00:05.000Z", "POINT (-78.0 35.0)")

        cache.put(sf3)

        cache.query("1") must beSome(sf3.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf3)
      }
    }

    "order by event time expression" in {
      val ex = EventTimeConfig(Duration.Inf, "dateToLong(dtg)", ordered = true)
      val config = IndexConfig(ex, res, Seq.empty, Seq.empty, lazyDeserialization = true, None)

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        val sf1 = ScalaSimpleFeature.create(sft, "1", "first", "2018-01-01T12:00:00.000Z", "POINT (-78.0 35.0)")
        cache.put(sf1)

        val sf2 = ScalaSimpleFeature.create(sft, "1", "second", "2018-01-01T11:59:55.000Z", "POINT (-78.0 35.0)")
        cache.put(sf2)

        cache.query("1") must beSome(sf1.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf1)

        val sf3 = ScalaSimpleFeature.create(sft, "1", "third", "2018-01-01T12:00:05.000Z", "POINT (-78.0 35.0)")

        cache.put(sf3)

        cache.query("1") must beSome(sf3.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf3)
      }
    }

    "order by message time" in {
      val config = IndexConfig(NeverExpireConfig, res, Seq.empty, Seq.empty, lazyDeserialization = true, None)

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        val sf1 = ScalaSimpleFeature.create(sft, "1", "first", "2018-01-01T12:00:00.000Z", "POINT (-78.0 35.0)")
        cache.put(sf1)

        val sf2 = ScalaSimpleFeature.create(sft, "1", "second", "2018-01-01T11:59:55.000Z", "POINT (-78.0 35.0)")
        cache.put(sf2)

        cache.query("1") must beSome(sf2.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf2)

        val sf3 = ScalaSimpleFeature.create(sft, "1", "third", "2018-01-01T12:00:05.000Z", "POINT (-78.0 35.0)")

        cache.put(sf3)

        cache.query("1") must beSome(sf3.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf3)
      }
    }

    "expire by event time with ordering" in {
      val ex = mock[ScheduledExecutorService]
      val ticker = new MockTicker()
      val ev = EventTimeConfig(Duration("100ms"), "dtg", ordered = true)
      val config = IndexConfig(ev, res, Seq.empty, Seq.empty, lazyDeserialization = true, Some((ex, ticker)))

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        val sf1 = ScalaSimpleFeature.create(sft, "1", "first", new Date(ticker.millis), "POINT (-78.0 35.0)")

        val expire1 = new WrappedRunnable(100L)
        ex.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
          expire1.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
          new ScheduledExpiry(expire1)
        }
        cache.put(sf1)
        expire1.runnable must not(beNull)
        there was one(ex).schedule(ArgumentMatchers.eq(expire1.runnable), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

        // move time forward
        ticker.millis += 50L

        val sf2 = ScalaSimpleFeature.create(sft, "1", "second", new Date(ticker.millis - 1000), "POINT (-78.0 35.0)")
        cache.put(sf2)
        expire1.cancelled must beFalse

        cache.query("1") must beSome(sf1.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf1)

        // move time forward and run the expiration
        ticker.millis += 100L
        expire1.runnable.run()

        cache.query("1") must beNone
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")) must beEmpty

        // move time forward
        ticker.millis += 100L

        val sf3 = ScalaSimpleFeature.create(sft, "1", "third", new Date(ticker.millis - 10), "POINT (-78.0 35.0)")

        // expiration should be 90 millis based on the event time date
        val expire3 = new WrappedRunnable(90L)
        ex.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(90L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
          expire3.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
          new ScheduledExpiry(expire3)
        }
        cache.put(sf3)
        expire3.runnable must not(beNull)
        there was one(ex).schedule(ArgumentMatchers.eq(expire3.runnable), ArgumentMatchers.eq(90L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

        cache.query("1") must beSome(sf3.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf3)

        // move time forward and run the expiration
        ticker.millis += 100L
        expire3.runnable.run()

        cache.query("1") must beNone
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")) must beEmpty

        // verify that the second feature didn't trigger an expiration, as it was ignored due to event time
        there were two(ex).schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.anyLong(), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))
      }
    }

    "expire by event time with ordering (no mocking)" in {
      val ev = EventTimeConfig(Duration("100ms"), "dtg", ordered = true)
      val config = IndexConfig(ev, res, Seq.empty, Seq.empty, lazyDeserialization = true, None)

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        val sf1 = ScalaSimpleFeature.create(sft, "1", "first", new Date(), "POINT (-78.0 35.0)")
        cache.put(sf1)
        cache.query("1") must beSome(sf1.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf1)

        eventually(cache.query("1") must beNone)
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq must beEmpty
      }
    }

    "expire by event time without ordering" in {
      val ex = mock[ScheduledExecutorService]
      val ticker = new MockTicker()
      val ev = EventTimeConfig(Duration("100ms"), "dtg", ordered = false)
      val config = IndexConfig(ev, res, Seq.empty, Seq.empty, lazyDeserialization = true, Some((ex, ticker)))

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        val sf1 = ScalaSimpleFeature.create(sft, "1", "first", new Date(ticker.millis), "POINT (-78.0 35.0)")

        val expire1 = new WrappedRunnable(100L)
        ex.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
          expire1.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
          new ScheduledExpiry(expire1)
        }
        cache.put(sf1)
        expire1.runnable must not(beNull)
        there was one(ex).schedule(ArgumentMatchers.eq(expire1.runnable), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

        ticker.millis += 10L

        val sf2 = ScalaSimpleFeature.create(sft, "1", "second", new Date(ticker.millis - 50), "POINT (-78.0 35.0)")
        val expire2 = new WrappedRunnable(50L)
        ex.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(50L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
          expire2.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
          new ScheduledExpiry(expire2)
        }
        cache.put(sf2)
        expire2.runnable must not(beNull)
        there was one(ex).schedule(ArgumentMatchers.eq(expire2.runnable), ArgumentMatchers.eq(50L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))
        expire1.cancelled must beTrue

        cache.query("1") must beSome(sf2.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf2)

        // move time forward and run the expiration
        ticker.millis += 100L
        expire2.runnable.run()

        cache.query("1") must beNone
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")) must beEmpty

        val sf3 = ScalaSimpleFeature.create(sft, "1", "third", new Date(ticker.millis + 1000), "POINT (-78.0 35.0)")

        // expiration should be 1100 millis based on the event time date
        val expire3 = new WrappedRunnable(1100L)
        ex.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(1100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
          expire3.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
          new ScheduledExpiry(expire3)
        }
        cache.put(sf3)
        expire3.runnable must not(beNull)
        there was one(ex).schedule(ArgumentMatchers.eq(expire3.runnable), ArgumentMatchers.eq(1100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

        cache.query("1") must beSome(sf3.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf3)

        // move time forward and run the expiration
        ticker.millis += 1100L
        expire3.runnable.run()

        cache.query("1") must beNone
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")) must beEmpty
      }
    }

    "expire by advanced event time without ordering" in {
      val ex = mock[ScheduledExecutorService]
      val ticker = new MockTicker()
      val ev = FilteredExpiryConfig(Seq(
        "trackId = 'first'"  -> EventTimeConfig(Duration("100ms"), "dtg", ordered = false),
        "trackId = 'second'" -> EventTimeConfig(Duration("150ms"), "dtg", ordered = false),
        "INCLUDE"            -> EventTimeConfig(Duration("200ms"), "dtg", ordered = false)
      ))
      val config = IndexConfig(ev, res, Seq.empty, Seq.empty, lazyDeserialization = true, Some((ex, ticker)))

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        val sf1 = ScalaSimpleFeature.create(sft, "1", "first", new Date(ticker.millis), "POINT (-78.0 35.0)")

        val expire1 = new WrappedRunnable(100L)
        ex.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
          expire1.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
          new ScheduledExpiry(expire1)
        }
        cache.put(sf1)
        expire1.runnable must not(beNull)
        there was one(ex).schedule(ArgumentMatchers.eq(expire1.runnable), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

        ticker.millis += 10L

        val sf2 = ScalaSimpleFeature.create(sft, "1", "second", new Date(ticker.millis - 30), "POINT (-78.0 35.0)")
        val expire2 = new WrappedRunnable(120L)
        ex.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(120L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
          expire2.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
          new ScheduledExpiry(expire2)
        }
        cache.put(sf2)
        expire2.runnable must not(beNull)
        there was one(ex).schedule(ArgumentMatchers.eq(expire2.runnable), ArgumentMatchers.eq(120L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))
        expire1.cancelled must beTrue

        cache.query("1") must beSome(sf2.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf2)

        // move time forward and run the expiration
        ticker.millis += 200L
        expire2.runnable.run()

        cache.query("1") must beNone
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")) must beEmpty

        val sf3 = ScalaSimpleFeature.create(sft, "1", "third", new Date(ticker.millis + 1000), "POINT (-78.0 35.0)")

        // expiration should be 1200 millis based on the event time date
        val expire3 = new WrappedRunnable(1200L)
        ex.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(1200L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
          expire3.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
          new ScheduledExpiry(expire3)
        }
        cache.put(sf3)
        expire3.runnable must not(beNull)
        there was one(ex).schedule(ArgumentMatchers.eq(expire3.runnable), ArgumentMatchers.eq(1200L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

        cache.query("1") must beSome(sf3.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf3)

        // move time forward and run the expiration
        ticker.millis += 1200L
        expire3.runnable.run()

        cache.query("1") must beNone
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")) must beEmpty
      }
    }
  }
}
