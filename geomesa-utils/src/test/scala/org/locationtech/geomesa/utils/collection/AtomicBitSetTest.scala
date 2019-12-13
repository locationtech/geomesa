/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import java.util.concurrent.CountDownLatch

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class AtomicBitSetTest extends Specification {

  "AtomicBitSet" should {
    "set and get values" >> {
      val bs = AtomicBitSet(16)
      foreach(0 until 16) { i =>
        bs.contains(i) must beFalse
        bs.add(i) must beTrue
        bs.contains(i) must beTrue
        bs.add(i) must beFalse
      }
      foreach(0 until 16) { i =>
        bs.remove(i) must beTrue
        bs.contains(i) must beFalse
        bs.remove(i) must beFalse
        bs.contains(i) must beFalse
      }
    }
    "clear values" >> {
      val bs = AtomicBitSet(16)
      foreach(0 until 16) { i =>
        bs.add(i) must beTrue
      }
      foreach(0 until 16) { i =>
        bs.contains(i) must beTrue
      }
      bs.clear()
      foreach(0 until 16) { i =>
        bs.contains(i) must beFalse
      }
    }
    "have the correct size" >> {
      foreach(Seq(32, 64, 96)) { i =>
        val bs = AtomicBitSet(i)
        bs.add(i - 1) must beTrue
        bs.add(i) must throwAn[IndexOutOfBoundsException]
      }
    }
    "handle concurrent requests" >> {
      val r = new Random(-7)
      val ints = (0 until 16 * 5).map(_ => r.nextInt(16)).toArray
      ints.toSeq must containAllOf(0 until 16)
      val bs = AtomicBitSet(16)
      val counts = new CountDownLatch(16)
      val threads = (0 until 16).map { t =>
        val runnable = new Runnable {
          override def run(): Unit = {
            (0 until 5).foreach { i =>
              if (bs.add(ints(5 * t + i))) {
                counts.countDown()
              }
            }
          }
        }
        new Thread(runnable)
      }
      threads.foreach(_.start())
      threads.foreach(_.join(1000))
      counts.getCount mustEqual 0
      foreach(0 until 16)(bs.contains(_) must beTrue)
    }
  }
}
