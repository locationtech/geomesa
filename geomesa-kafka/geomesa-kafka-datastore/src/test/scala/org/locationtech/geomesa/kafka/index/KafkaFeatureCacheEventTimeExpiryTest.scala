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
class KafkaFeatureCacheEventTimeExpiryTest extends Specification {
  private val sft = SimpleFeatureTypes.createType("track", "trackId:String,dtg:Date:default=true,*geom:Point:srid=4326")
  private val gf = JTSFactoryFinder.getGeometryFactory
  private val ff = CommonFactoryFinder.getFilterFactory2
  private val expr = ff.function("fastProperty", ff.literal(sft.indexOf("dtg")))

  sequential

  "KafkaFeatureCache" should {
    val ticker = new MockTicker

    val cache = new FeatureCacheGuava(sft, Duration("100s"), true, expr, Duration.Inf, Duration.Inf)(ticker)

    "Respect event-time expiry" in {
      sequential

      val now = Instant.now()
      ticker.millis = now.toEpochMilli
      val sf = ScalaSimpleFeature.create(sft, "1", "first", Date.from(now), gf.createPoint(new Coordinate(0, 0)))

      cache.put(sf)

      ticker.millis += 99*1000

      cache.cleanUp()

      val firstCheck = cache.query("1").isDefined
      ticker.millis += 2*1000

      cache.cleanUp()

      val secondCheck = cache.query("1").isDefined
      // create a feature that is 80 seconds behind the current ticker
      val sf2 = ScalaSimpleFeature.create(sft, "1", "first", Date.from(Instant.ofEpochMilli(ticker.millis).minusSeconds(80)), gf.createPoint(new Coordinate(0, 0)))

      cache.put(sf2)

      // advance the ticker 19 seconds
      ticker.millis += 19*1000
      cache.cleanUp()

      val thirdCheck = cache.query("1").isDefined

      // advance the ticker 2 more seconds so the 80+21 = 101 seconds exceeds the 100s timeout
      ticker.millis += 2*1000
      cache.cleanUp()

      val fourthCheck = cache.query("1").isDefined

      // create a feature in the future
      val sf3 = ScalaSimpleFeature.create(sft, "1", "first", Date.from(Instant.ofEpochMilli(ticker.millis).plusSeconds(80)), gf.createPoint(new Coordinate(0, 0)))

      cache.put(sf3)

      // advance the ticker plus 81 seconds to the current time of the feature + 1s (F.T+1), feature should still be there
      ticker.millis += 81*1000
      cache.cleanUp()
      val fifthCheck = cache.query("1").isDefined

      // advance the ticker to beyond 100s from first write of sf3 (F.T+21), feature should still be there
      ticker.millis += 20*1000
      cache.cleanUp()
      val sixthCheck = cache.query("1").isDefined

      // advance the ticker to just before 100s from the time of the feature (F.T+99), feature should still be there
      ticker.millis += 78*1000
      cache.cleanUp()
      val seventhCheck = cache.query("1").isDefined

      // advance the ticker to just after 100s from the time of the feature (F.T+101), feature should not still be there
      ticker.millis += 2*1000
      cache.cleanUp()
      val eighthCheck = cache.query("1").isDefined

      // test an already expired entry
      val sf4 = ScalaSimpleFeature.create(sft, "1", "first", Date.from(Instant.ofEpochMilli(ticker.millis).minusSeconds(101)), gf.createPoint(new Coordinate(0, 0)))

      cache.put(sf4)
      cache.cleanUp()
      val alreadyExpired = cache.query("1").isDefined

      // test an update
      val sf5 = ScalaSimpleFeature.create(sft, "1", "first", Date.from(Instant.ofEpochMilli(ticker.millis)), gf.createPoint(new Coordinate(0, 0)))
      cache.put(sf5)

      // advance the ticker half way and update the entry
      ticker.millis += 50*1000
      val sf6 = ScalaSimpleFeature.create(sft, "1", "second", Date.from(Instant.ofEpochMilli(ticker.millis)), gf.createPoint(new Coordinate(0, 0)))
      cache.put(sf6)

      // advance the ticker another 50 seconds and feature should still exist
      ticker.millis += 51*1000
      cache.cleanUp()
      val update1 = cache.query("1").isDefined

      // advance the ticker another 50 seconds and feature should not exist
      ticker.millis += 51*1000
      cache.cleanUp()
      val update2 = cache.query("1").isDefined

      "first check should return the entry" >> { firstCheck must beTrue }
      "second check should not return the entry" >> { secondCheck must beFalse }
      "entry should still be there" >> {  thirdCheck must beTrue }
      "entry should not be there" >> { fourthCheck must beFalse }
      "fifth" >> { fifthCheck must beTrue }
      "six" >> { sixthCheck must beTrue }
      "seven" >> { seventhCheck must beTrue }
      "eight" >> { eighthCheck must beFalse }
      "alreadyExpired" >> { alreadyExpired must beFalse }
      "update1" >> { update1 must beTrue }
      "update2" >> { update2 must beFalse }
    }

    "deal with sft's without a dtg" in {
      val sft = SimpleFeatureTypes.createType("track", "trackId:String,dtg:String:default=true,*geom:Point:srid=4326")
      val expr = ff.function("longToDate", ff.function("parseLong", ff.function("fastProperty", ff.literal(sft.indexOf("dtg")))))

      implicit val ticker = new MockTicker
      ticker.millis = Instant.now().toEpochMilli
      val cache = new FeatureCacheGuava(sft, Duration("100s"), true, expr, Duration.Inf, Duration.Inf)(ticker)

      val sf = ScalaSimpleFeature.create(sft, "1", "first", Instant.now().getEpochSecond.toString, gf.createPoint(new Coordinate(0, 0)))

      cache.put(sf)
      val check1 = cache.query("1").isDefined

      // advance ticker
      ticker.millis += 100*1000
      cache.cleanUp()
      val check2 = cache.query("1").isDefined
      "feature must be there" >> {  check1 must beTrue }
      "feature must not be there" >> {  check2 must beFalse }
    }
  }
}
