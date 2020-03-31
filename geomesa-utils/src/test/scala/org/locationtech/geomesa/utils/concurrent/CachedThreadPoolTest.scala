/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.concurrent

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CachedThreadPoolTest extends Specification {

  "CachedThreadPool" should {
    "execute concurrent tasks up to the thread limit" in {
      val pool = new CachedThreadPool(2)
      val latch = new CountDownLatch(1)
      val started1 = new AtomicBoolean(false)
      val started2 = new AtomicBoolean(false)
      val started3 = new AtomicBoolean(false)
      val thread1 = new Runnable() { override def run(): Unit = { started1.set(true); latch.await() } }
      val thread2 = new Runnable() { override def run(): Unit = { started2.set(true); latch.await() } }
      val thread3 = new Runnable() { override def run(): Unit = { started3.set(true); latch.await() } }
      pool.submit(thread1)
      pool.submit(thread2)
      pool.submit(thread3)
      pool.shutdown()
      // verify threads 1 and 2 started
      eventually(started1.get must beTrue)
      eventually(started2.get must beTrue)
      Thread.sleep(100)
      // verify thread 3 hasn't started
      started3.get must beFalse
      latch.countDown() // release the running threads
      // verify thread 3 ran
      eventually(started3.get must beTrue)
    }
    "terminate running and future tasks on shutdownNow" in {
      val pool = new CachedThreadPool(1)
      val latch = new CountDownLatch(1)
      val started1 = new AtomicBoolean(false)
      val started2 = new AtomicBoolean(false)
      val ended1 = new AtomicBoolean(false)
      val ended2 = new AtomicBoolean(false)
      val thread1 = new Runnable() { override def run(): Unit = { started1.set(true); latch.await(); ended1.set(true) } }
      val thread2 = new Runnable() { override def run(): Unit = { started2.set(true); latch.await(); ended2.set(true) } }
      pool.submit(thread1)
      pool.submit(thread2)
      pool.shutdown()
      eventually(started1.get must beTrue) // verify thread 1 has started
      pool.shutdownNow().size() mustEqual 1 // the one thread that was running
      pool.awaitTermination(1, TimeUnit.SECONDS)
      pool.isTerminated must beTrue
      // verify thread 2 never started, and thread 1 was interrupted before finishing
      forall(Seq(ended1, started2, ended2))(_.get must beFalse)
    }
    "await termination" in {
      val pool = new CachedThreadPool(2)
      val latch = new CountDownLatch(1)
      val start = new AtomicBoolean(false)
      val done = new AtomicBoolean(false)
      val waiter = Executors.newSingleThreadExecutor()
      waiter.submit(new Runnable() { override def run(): Unit = {
        start.set(true)
        done.set(pool.awaitTermination(1, TimeUnit.MINUTES))
      }})
      waiter.shutdown()
      eventually(start.get must beTrue)

      pool.submit(new Runnable() { override def run(): Unit = { latch.await() } })
      pool.submit(new Runnable() { override def run(): Unit = { latch.await() } })
      pool.submit(new Runnable() { override def run(): Unit = { latch.await() } })
      pool.shutdown()

      pool.isTerminated must beFalse

      latch.countDown()

      eventually(done.get must beTrue)
      pool.isTerminated must beTrue
    }
  }
}
