/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import com.github.benmanes.caffeine.cache.Ticker
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.ArgumentMatchers
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThreadLocalCacheTest extends Specification with Mockito {

  import scala.concurrent.duration._

  "ThreadLocalCache" should {
    "implement map methods" in {
      new ThreadLocalCache[String, String](10.minutes) must
          beAnInstanceOf[scala.collection.mutable.Map[String, String]]
    }

    "allow getOrElseUpdate" in {
      WithClose(new ThreadLocalCache[String, String](10.minutes)) { cache =>
        cache.getOrElseUpdate("k1", "v1") mustEqual "v1"
        var sideEffect = "1"
        cache.getOrElseUpdate("k1", { sideEffect = "2"; "v1" }) mustEqual "v1"
        sideEffect mustEqual "1"
      }
    }

    "be thread safe" in {
      WithClose(new ThreadLocalCache[String, AnyRef](10.minutes)) { cache =>
        val obj1 = new AnyRef()
        val obj2 = new AnyRef()
        val first = cache.getOrElseUpdate("k1", obj1)
        var second: AnyRef = null
        val t = new Thread(new Runnable() { override def run(): Unit = second = cache.getOrElseUpdate("k1", obj2) } )
        t.start()
        t.join()
        first must beTheSameAs(obj1)
        second must beTheSameAs(obj2)
        first must not beTheSameAs second
      }
    }

    "read expired references correctly" in {
      val es = mock[MockScheduledExecutorService]
      val future = mock[ScheduledFuture[Unit]]
      es.scheduleWithFixedDelay(ArgumentMatchers.any(), ArgumentMatchers.eq(100L),
        ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) returns future
      var nanos = 0L
      val ticker = new Ticker() { override def read(): Long = nanos }
      WithClose(new ThreadLocalCache[String, String](100.millis, es, Some(ticker))) { cache =>
        there was one(es).scheduleWithFixedDelay(cache, 100, 100, TimeUnit.MILLISECONDS)
        cache.put("k1", "v1")
        cache.get("k1") must beSome("v1")
        nanos = 200L * 1000000
        cache.get("k1") must beNone
      }
      there was one(future).cancel(true)
    }

    "update expired references correctly" in {
      val es = mock[MockScheduledExecutorService]
      val future = mock[ScheduledFuture[Unit]]
      es.scheduleWithFixedDelay(ArgumentMatchers.any(), ArgumentMatchers.eq(100L),
        ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) returns future
      var nanos = 0L
      val ticker = new Ticker() { override def read(): Long = nanos }
      WithClose(new ThreadLocalCache[String, String](100.millis, es, Some(ticker))) { cache =>
        there was one(es).scheduleWithFixedDelay(cache, 100, 100, TimeUnit.MILLISECONDS)
        cache.put("k1", "v1")
        cache.get("k1") must beSome("v1")
        nanos = 200L * 1000000
        cache.get("k1") must beNone
        cache.getOrElseUpdate("k1", "v2") mustEqual "v2"
        cache.get("k1") must beSome("v2")
        cache("k1") mustEqual "v2"
      }
      there was one(future).cancel(true)
    }
  }

  // needed to handle mocking the unbound wildcard in the signature for scheduleWithFixedDelay
  trait MockScheduledExecutorService extends ScheduledExecutorService {
    override def scheduleWithFixedDelay(command: Runnable, initialDelay: Long, delay: Long, unit: TimeUnit): ScheduledFuture[Unit]
  }
}
