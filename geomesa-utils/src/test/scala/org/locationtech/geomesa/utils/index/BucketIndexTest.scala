/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Envelope, Point}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BucketIndexTest extends Specification with LazyLogging {

  "BucketIndex" should {
    "be thread safe" in {
      val numFeatures = 100
      val envelopes = (0 until numFeatures).map(i => (i, WKTUtils.read(s"POINT(45.$i 50)").getEnvelopeInternal)).toArray
      val index = new BucketIndex[Int]()
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
            index.insert(envelopes(i)._2, i.toString, i)
            inserts += 1
          }
        }
      })
      es.submit(new Runnable(){
        override def run(): Unit = {
          val r = new Random
          while (running.get) {
            val i = r.nextInt(numFeatures)
            index.query(envelopes(i)._2).foreach(_ => Unit)
            queries += 1
          }
        }
      })
      es.submit(new Runnable(){
        override def run(): Unit = {
          val r = new Random
          while (running.get) {
            val i = r.nextInt(numFeatures)
            index.remove(envelopes(i)._2, i.toString)
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
      val index = new BucketIndex[String]()
      val pts = for (x <- -180 to 180; y <- -90 to 90) yield {
        s"POINT($x $y)"
      }
      pts.foreach { pt =>
        val env = WKTUtils.read(pt).getEnvelopeInternal
        index.insert(env, pt, pt)
      }
      pts.foreach { pt =>
        val env = WKTUtils.read(pt).getEnvelopeInternal
        val results = index.query(env).toSeq
        results must contain(pt)
      }
      success
    }

    "support insert by point and query" in {
      val index = new BucketIndex[Point]()
      val pts = for (x <- -180 to 180; y <- -90 to 90) yield { WKTUtils.read(s"POINT($x $y)").asInstanceOf[Point] }
      pts.foreach { pt =>
        index.insert(pt.getX, pt.getY, pt.toString, pt)
      }
      pts.foreach { pt =>
        val env = pt.getEnvelopeInternal
        val results = index.query(env).toSeq
        results must contain(pt)
      }
      success
    }

    "support envelopes" in {
      val index = new BucketIndex[String]()
      val pts = for (x <- -180 to 180; y <- -90 to 90) yield {
        s"POINT($x $y)"
      }
      pts.foreach { pt =>
        val env = WKTUtils.read(pt).getEnvelopeInternal
        index.insert(env, pt, pt)
      }
      val bbox = new Envelope(-10, -8, 8, 10)
      val results = index.query(bbox).toSeq
      results must haveLength(9)
      results must containTheSameElementsAs(for (x <- -10 to -8; y <- 8 to 10) yield s"POINT($x $y)")

      bbox.init(-10.5, -8.5, 8.5, 10.5)
      val results2 = index.query(bbox).toSeq
      // fine grain filtering is not applied - we want everything that *might* intersect
      results2 must haveLength(9)
      results2 must containTheSameElementsAs(for (x <- -11 to -9; y <- 8 to 10) yield s"POINT($x $y)")

      val results3 = index.query(bbox).toSeq.filter { s =>
        val x = s.substring(6, s.indexOf(" ")).toInt
        val y = s.substring(s.indexOf(" ") + 1, s.length - 1).toInt
        x > -10.5 && x < -8.5 && y > 8.5 && y < 10.5
      }
      results3 must haveLength(4)
      results3 must containTheSameElementsAs(for (x <- -10 to -9; y <- 9 to 10) yield s"POINT($x $y)")
    }
  }
}


