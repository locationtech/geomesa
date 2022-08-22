/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Envelope
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class SizeSeparatedBucketIndexTest extends Specification with LazyLogging {

  "BucketIndex" should {
    "be thread safe" in {
      val numFeatures = 100
      val points = (0 until numFeatures).map(i => (i, WKTUtils.read(s"POINT(45.$i 50)"))).toArray
      val index = new SizeSeparatedBucketIndex[Int]()
      val running = new AtomicBoolean(true)

      var inserts = 0L
      var queries = 0L
      var removes = 0L

      val es = Executors.newFixedThreadPool(3)
      es.submit(new Runnable(){
        override def run(): Unit = {
          val r = new Random
          while (running.get) {
            val i = r.nextInt(numFeatures)
            index.insert(points(i)._2, i.toString, i)
            inserts += 1
          }
        }
      })
      es.submit(new Runnable(){
        override def run(): Unit = {
          val r = new Random
          while (running.get) {
            val i = r.nextInt(numFeatures)
            index.query(points(i)._2.getEnvelopeInternal).foreach(_ => ())
            queries += 1
          }
        }
      })
      es.submit(new Runnable(){
        override def run(): Unit = {
          val r = new Random
          while (running.get) {
            val i = r.nextInt(numFeatures)
            index.remove(points(i)._2, i.toString)
            removes += 1
          }
        }
      })

      es.shutdown()
      Thread.sleep(1000)
      running.set(false)
      es.awaitTermination(1, TimeUnit.SECONDS)

      success
    }

    "support insert and query" in {
      val index = new SizeSeparatedBucketIndex[String]()
      val pts = for (x <- Range(-180, 180, 2); y <- Range(-90, 90, 2)) yield {
        s"POINT($x $y)"
      }
      pts.foreach { pt =>
        index.insert(WKTUtils.read(pt), pt, pt)
      }
      pts.foreach { pt =>
        val env = WKTUtils.read(pt).getEnvelopeInternal
        val results = index.query(env).toSeq
        results must contain(pt)
      }
      success
    }

    "support envelopes" in {
      val index = new SizeSeparatedBucketIndex[String]()
      val pts = for (x <- -180 to 180; y <- -90 to 90) yield {
        s"POINT($x $y)"
      }
      pts.foreach { pt =>
        index.insert(WKTUtils.read(pt), pt, pt)
      }
      val bbox = new Envelope(-10, -8, 8, 10)
      val results = index.query(bbox).toSeq
      results must containAllOf(for (x <- -10 to -8; y <- 8 to 10) yield s"POINT($x $y)")

      bbox.init(-10.5, -8.5, 8.5, 10.5)
      val results2 = index.query(bbox).toSeq
      // fine grain filtering is not applied - we want everything that *might* intersect
      results2 must containAllOf(for (x <- -11 to -9; y <- 8 to 10) yield s"POINT($x $y)")

      val results3 = index.query(bbox).toSeq.filter { s =>
        val x = s.substring(6, s.indexOf(" ")).toInt
        val y = s.substring(s.indexOf(" ") + 1, s.length - 1).toInt
        x > -10.5 && x < -8.5 && y > 8.5 && y < 10.5
      }
      results3 must haveLength(4)
      results3 must containTheSameElementsAs(for (x <- -10 to -9; y <- 9 to 10) yield s"POINT($x $y)")
    }

    "support geometries of different sizes" in {
      val index = new SizeSeparatedBucketIndex[String]()

      index.insert(WKTUtils.read("POLYGON((-10 5, -10 20, 5 20, 5 5, -10 5))"), "foo", "foo")

      for (x <- -10 to 5; y <- 5 to 20) {
        index.query(x - 0.1, y - 0.1, x + 0.1, y + 0.1).toSeq mustEqual Seq("foo")
      }

      index.insert(WKTUtils.read("POLYGON((0 7, 0 8, 3 8, 3 7, 0 7))"), "bar", "bar")
      for (x <- 0 to 3; y <- 7 to 8) {
        index.query(x - 0.1, y - 0.1, x + 0.1, y + 0.1).toSeq must containTheSameElementsAs(Seq("foo", "bar"))
      }

      index.query(-150, -60, -149, -59).toSeq must beEmpty
      index.query(-180, -90, -149, -59).toSeq must beEmpty
    }
  }
}


