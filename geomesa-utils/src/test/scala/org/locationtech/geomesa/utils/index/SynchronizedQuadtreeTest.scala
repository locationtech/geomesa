/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Lock

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Point
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class SynchronizedQuadtreeTest extends Specification with LazyLogging {

  "SynchronizedQuadtree" should {

    "be thread safe" in {
      val qt = new SynchronizedQuadtree[Point]
      val pt = WKTUtils.read("POINT(45 50)").asInstanceOf[Point]
      val env = pt.getEnvelopeInternal
      val wholeWorld = WKTUtils.read("POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))").getEnvelopeInternal
      val t1 = new Thread(new Runnable() {
        override def run() = {
          var i = 0
          while (i < 1000) {
            qt.insert(env, pt)
            Thread.sleep(1)
            i += 1
          }
        }
      })
      t1.start()
      var i = 0
      while (i < 1000) {
        qt.query(wholeWorld)
        Thread.sleep(1)
        i += 1
      }
      t1.join()
      success
    }

    "support high throughput" in {

      skipped("integration")

      val qt = new SynchronizedQuadtreeWithMetrics
      val rand = new Random(-75)

      // pre-populate with some data
      val points = (1 to 999999).map { _ =>
        val (x, y) = (rand.nextInt(360) - 180 + rand.nextDouble(), rand.nextInt(180) - 90 + rand.nextDouble())
        WKTUtils.read(s"POINT($x $y)").asInstanceOf[Point]
      }
      points.foreach(pt => qt.insert(pt.getEnvelopeInternal, pt))
      qt.writeWait.set(0)
      qt.totalWrites.set(0)

      val wholeWorld = WKTUtils.read("POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))").getEnvelopeInternal

      val endTime = System.currentTimeMillis() + 10000 // 10s

      // 12 writers write a random point every ~5ms
      val writers = (1 to 12).map(_ => new Thread(new Runnable() {
        override def run() = {
          while (System.currentTimeMillis() < endTime) {
            Thread.sleep(rand.nextInt(10))
            val (x, y) = (rand.nextInt(360) - 180 + rand.nextDouble(), rand.nextInt(180) - 90 + rand.nextDouble())
            val pt = WKTUtils.read(s"POINT($x $y)").asInstanceOf[Point]
            qt.insert(pt.getEnvelopeInternal, pt)
          }
        }
      }))
      // 2 deleters delete a random point every ~100ms
      val deleters = (1 to 2).map(_ => new Thread(new Runnable() {
        override def run() = {
          while (System.currentTimeMillis() < endTime) {
            Thread.sleep(rand.nextInt(200))
            val pt = points(rand.nextInt(points.size))
            qt.remove(pt.getEnvelopeInternal, pt)
          }
        }
      }))
      // 12 readers read the whole world every ~100ms
      val readers = (1 to 12).map(_ => new Thread(new Runnable() {
        override def run() = {
          while (System.currentTimeMillis() < endTime) {
            Thread.sleep(rand.nextInt(200))
            qt.query(wholeWorld)
          }
        }
      }))

      val allThreads = readers ++ writers ++ deleters
      allThreads.foreach(_.start())
      allThreads.foreach(_.join())

      println("Total reads: " + qt.totalReads.get)
      println("Read rate: " + (qt.totalReads.get / 10) + "/s")
      println("Average time waiting for read: " + (qt.readWait.get / qt.totalReads.get) + "ms")
      println("Max time waiting for read: " + qt.maxReadWait.get + "ms")

      println("Total writes: " + qt.totalWrites.get)
      println("Write rate: " + (qt.totalWrites.get / 10) + "/s")
      println("Average time waiting for write: " + (qt.writeWait.get / qt.totalWrites.get) + "ms")
      println("Max time waiting for write: " + qt.maxWriteWait.get + "ms")
      println()

      // Average results:

      // Total reads: 1670
      // Read rate: 167/s
      // Average time waiting for read: 13ms
      // Max time waiting for read: 150ms
      // Total writes: 4141
      // Write rate: 414/s
      // Average time waiting for write: 25ms
      // Max time waiting for write: 163ms

      success
    }
  }
}

class SynchronizedQuadtreeWithMetrics extends SynchronizedQuadtree[Point] {

  val readWait = new AtomicLong()
  val writeWait = new AtomicLong()
  val maxReadWait = new AtomicLong()
  val maxWriteWait = new AtomicLong()
  val totalReads = new AtomicLong()
  val totalWrites = new AtomicLong()

  override protected [index] def withLock[U](lock: Lock)(fn: => U): U = {
    val start = System.currentTimeMillis()
    lock.lock()
    val time = System.currentTimeMillis() - start
    if (lock == readLock) {
      readWait.addAndGet(time)
      totalReads.incrementAndGet()
      var max = maxReadWait.get()
      while (max < time && maxReadWait.compareAndSet(max, time)) {
        max = maxReadWait.get()
      }
    } else {
      writeWait.addAndGet(time)
      totalWrites.incrementAndGet()
      var max = maxWriteWait.get()
      while (max < time && maxWriteWait.compareAndSet(max, time)) {
        max = maxWriteWait.get()
      }
    }
    try {
      fn
    } finally {
      lock.unlock()
    }
  }
}
