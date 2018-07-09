/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.util.Date

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.data.KafkaDataStore.{EventTimeConfig, IndexConfig}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class EventTimeFeatureCacheTest extends Specification {

  sequential // sequential helps expiration timing to be more consistent

  val sft = SimpleFeatureTypes.createType("track", "trackId:String,dtg:Date:default=true,*geom:Point:srid=4326")

  "EventTimeFeatureCache" should {
    "order by event time" in {
      val ev = Some(EventTimeConfig("dtg", ordering = true))
      val config = IndexConfig(Duration.Inf, ev, 180, 90, Seq.empty, lazyDeserialization = true)

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        cache must beAnInstanceOf[EventTimeFeatureCache]

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
      val ev = Some(EventTimeConfig("dateToLong(dtg)", ordering = true))
      val config = IndexConfig(Duration.Inf, ev, 180, 90, Seq.empty, lazyDeserialization = true)

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        cache must beAnInstanceOf[EventTimeFeatureCache]

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
      val ev = Some(EventTimeConfig("dtg", ordering = false))
      val config = IndexConfig(Duration.Inf, ev, 180, 90, Seq.empty, lazyDeserialization = true)

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        cache must beAnInstanceOf[EventTimeFeatureCache]

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
      val ev = Some(EventTimeConfig("dtg", ordering = true))
      val config = IndexConfig(Duration("100ms"), ev, 180, 90, Seq.empty, lazyDeserialization = true)

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        cache must beAnInstanceOf[EventTimeFeatureCache]

        val sf1 = ScalaSimpleFeature.create(sft, "1", "first", new Date(), "POINT (-78.0 35.0)")
        cache.put(sf1)

        val sf2 = ScalaSimpleFeature.create(sft, "1", "second", new Date(System.currentTimeMillis() - 1000), "POINT (-78.0 35.0)")
        cache.put(sf2)

        cache.query("1") must beSome(sf1.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf1)

        cache.query("1") must eventually(beNone)
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")) must beEmpty

        val sf3 = ScalaSimpleFeature.create(sft, "1", "third", new Date(System.currentTimeMillis() + 1000), "POINT (-78.0 35.0)")

        cache.put(sf3)

        cache.query("1") must beSome(sf3.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf3)

        Thread.sleep(200)

        cache.query("1") must beSome(sf3.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf3)

        cache.query("1") must eventually(beNone)
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")) must beEmpty
      }
    }

    "expire by event time without ordering" in {
      val ev = Some(EventTimeConfig("dtg", ordering = false))
      val config = IndexConfig(Duration("100ms"), ev, 180, 90, Seq.empty, lazyDeserialization = true)

      WithClose(KafkaFeatureCache(sft, config)) { cache =>
        cache must beAnInstanceOf[EventTimeFeatureCache]

        val sf1 = ScalaSimpleFeature.create(sft, "1", "first", new Date(), "POINT (-78.0 35.0)")
        cache.put(sf1)

        val sf2 = ScalaSimpleFeature.create(sft, "1", "second", new Date(System.currentTimeMillis() - 10), "POINT (-78.0 35.0)")
        cache.put(sf2)

        cache.query("1") must beSome(sf2.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf2)

        cache.query("1") must eventually(beNone)
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")) must beEmpty

        val sf3 = ScalaSimpleFeature.create(sft, "1", "third", new Date(System.currentTimeMillis() + 1000), "POINT (-78.0 35.0)")

        cache.put(sf3)

        cache.query("1") must beSome(sf3.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf3)

        Thread.sleep(200)

        cache.query("1") must beSome(sf3.asInstanceOf[SimpleFeature])
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")).toSeq mustEqual Seq(sf3)

        cache.query("1") must eventually(beNone)
        cache.query(ECQL.toFilter("bbox(geom,-79.0,34.0,-77.0,36.0)")) must beEmpty
      }
    }
  }
}
