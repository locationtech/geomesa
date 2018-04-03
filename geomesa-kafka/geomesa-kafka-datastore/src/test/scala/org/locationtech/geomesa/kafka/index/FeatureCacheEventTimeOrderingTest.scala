/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.time.Instant
import java.util.Date

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.MockTicker
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class FeatureCacheEventTimeOrderingTest extends Specification {

  private val sft = SimpleFeatureTypes.createType("track", "trackId:String,dtg:Date:default=true,*geom:Point:srid=4326")
  private val gf = JTSFactoryFinder.getGeometryFactory
  private val ff = CommonFactoryFinder.getFilterFactory2
  private val expr = ff.function("fastProperty", ff.literal(sft.indexOf("dtg")))

  "FeatureCacheEventTimeOrdering" should {
    "respect event time when used with guava" >> {
      implicit val ticker = new MockTicker
      val delegate = new FeatureCacheGuava(sft, Duration.Inf, false, null)

      val cache = new FeatureCacheEventTimeOrdering(delegate, expr)

      val now = Instant.now()

      val sf = ScalaSimpleFeature.create(sft, "1", "first", Date.from(now), gf.createPoint(new Coordinate(-78.0, 35.0)))

      cache.put(sf)

      val sf2 = ScalaSimpleFeature.create(sft, "1", "second", Date.from(now.minusSeconds(5000)), gf.createPoint(new Coordinate(-78.0, 35.0)))

      cache.put(sf2)

      val firstQuery = cache.query("1").get.getAttribute("trackId")

      val sf3 = ScalaSimpleFeature.create(sft, "1", "third", Date.from(now.plusSeconds(5000)), gf.createPoint(new Coordinate(-78.0, 35.0)))

      cache.put(sf3)

      val secondQuery = cache.query("1").get.getAttribute("trackId")

      "second put must be ignored" >> { firstQuery must be equalTo "first" }
      "third put must be respected" >> { secondQuery must be equalTo "third"}
    }

    "respect event time when used with cqengine" >> {
      implicit val ticker = new MockTicker
      val delegate = new FeatureCacheCqEngine(sft, Duration.Inf, false, null)
      val cache = new FeatureCacheEventTimeOrdering(delegate, expr)

      val now = Instant.now()

      val sf = ScalaSimpleFeature.create(sft, "1", "first", Date.from(now), gf.createPoint(new Coordinate(-78.0, 35.0)))

      cache.put(sf)

      val sf2 = ScalaSimpleFeature.create(sft, "1", "second", Date.from(now.minusSeconds(5000)), gf.createPoint(new Coordinate(-78.0, 35.0)))

      cache.put(sf2)

      val firstQuery = cache.query("1").get.getAttribute("trackId")

      val sf3 = ScalaSimpleFeature.create(sft, "1", "third", Date.from(now.plusSeconds(5000)), gf.createPoint(new Coordinate(-78.0, 35.0)))

      cache.put(sf3)

      val secondQuery = cache.query("1").get.getAttribute("trackId")

      "second put must be ignored" >> { firstQuery must be equalTo "first" }
      "third put must be respected" >> { secondQuery must be equalTo "third"}
    }
  }
}
