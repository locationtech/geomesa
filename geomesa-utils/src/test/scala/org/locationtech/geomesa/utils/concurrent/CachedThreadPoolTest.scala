/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.concurrent

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class CachedThreadPoolTest extends Specification {

  "CachedThreadPool" should {
    "execute concurrent tasks up to the thread limit" in {
      val pool = new CachedThreadPool(2)
      val latch = new CountDownLatch(1)
      val v1 = new AtomicBoolean(false)
      val v2 = new AtomicBoolean(false)
      val v3 = new AtomicBoolean(false)
      val t1 = new Runnable() { override def run(): Unit = { v1.set(true); latch.await() } }
      val t2 = new Runnable() { override def run(): Unit = { v2.set(true); latch.await() } }
      val t3 = new Runnable() { override def run(): Unit = { v3.set(true); latch.await() } }
      pool.submit(t1)
      pool.submit(t2)
      pool.submit(t3)
      pool.shutdown()
      // verify threads 1 and 2 started
      eventually(v1.get must beTrue)
      eventually(v2.get must beTrue)
      Thread.sleep(100)
      // verify thread 3 hasn't started
      v3.get must beFalse
      latch.countDown() // release the running threads
      // verify thread 3 ran
      eventually(v3.get must beTrue)
    }
    "terminate running and future tasks on shutdownNow" in {
      val pool = new CachedThreadPool(1)
      val latch = new CountDownLatch(1)
      val s1 = new AtomicBoolean(false)
      val s2 = new AtomicBoolean(false)
      val e1 = new AtomicBoolean(false)
      val e2 = new AtomicBoolean(false)
      val t1 = new Runnable() { override def run(): Unit = { s1.set(true); latch.await(); e1.set(true) } }
      val t2 = new Runnable() { override def run(): Unit = { s2.set(true); latch.await(); e2.set(true) } }
      pool.submit(t1)
      pool.submit(t2)
      pool.shutdown()
      eventually(s1.get must beTrue) // verify thread 1 has started
      pool.shutdownNow().size() mustEqual 1
      pool.awaitTermination(1, TimeUnit.SECONDS)
      pool.isTerminated must beTrue
      // verify thread 2 never started, and thread 1 was interrupted before finishing
      forall(Seq(e1, s2, e1))(_.get must beFalse)
    }
  }
}
