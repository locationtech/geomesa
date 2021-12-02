/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
=======
>>>>>>> 029cb6b3b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8d6c6b4169 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f8c087055e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ab986cc15d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 029cb6b3b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d6c6b4169 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f8c087055e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import org.apache.kafka.common.{Cluster, PartitionInfo}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore.FeatureIdPartitioner
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, Phaser}

@RunWith(classOf[JUnitRunner])
class FeatureIdPartitionerTest extends Specification {

  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"
  val sft = SimpleFeatureTypes.createType("test", spec)
  val serializer = KryoFeatureSerializer(sft)
  lazy val sf0 = serializer.serialize(ScalaSimpleFeature.create(sft, "4"))
  lazy val sf1 = serializer.serialize(ScalaSimpleFeature.create(sft, "1"))

  lazy val cluster =
    new Cluster("",
      Collections.emptyList(),
      {
        val partitions = new java.util.ArrayList[PartitionInfo]()
        partitions.add(new PartitionInfo("foo", 0, null, Array.empty, Array.empty))
        partitions.add(new PartitionInfo("foo", 1, null, Array.empty, Array.empty))
        partitions
      },
      Collections.emptySet(),
      Collections.emptySet(),
      Collections.emptySet(),
      null
    )

  "FeatureIdPartitioner" should {
    "partition based on feature id" in {
      val partitioner = new FeatureIdPartitioner()
      partitioner.configure(Collections.singletonMap(KafkaStore.SimpleFeatureSpecConfig, spec))

      partitioner.partition("foo", null, null, null, sf0, cluster) mustEqual 0
      partitioner.partition("foo", null, null, null, sf1, cluster) mustEqual 1
    }
    "not error if no partitions" in {
      val cluster =
        new Cluster("",
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptySet(),
          Collections.emptySet(),
          Collections.emptySet(),
          null
        )

      val partitioner = new FeatureIdPartitioner()
      partitioner.configure(Collections.singletonMap(KafkaStore.SimpleFeatureSpecConfig, spec))

      partitioner.partition("foo", null, null, null, sf0, cluster) mustEqual 0
      partitioner.partition("foo", null, null, null, sf1, cluster) mustEqual 0
      foreach(0 until 10) { i =>
        val sf = ScalaSimpleFeature.create(sft, "4", s"name$i", s"2020-01-01T0$i:00:00Z", s"POINT($i 10)")
        partitioner.partition("foo", null, null, null, serializer.serialize(sf), cluster) mustEqual 0
      }

    }
    "support multi-threading" in {
      val partitioner = new FeatureIdPartitioner()
      partitioner.configure(Collections.singletonMap(KafkaStore.SimpleFeatureSpecConfig, spec))

      val res0 = Collections.newSetFromMap(new ConcurrentHashMap[Int, java.lang.Boolean]())
      val res1 = Collections.newSetFromMap(new ConcurrentHashMap[Int, java.lang.Boolean]())
      val errors = new AtomicInteger()
      val phaser = new Phaser(3)

      CachedThreadPool.submit(new Runnable() {
        override def run(): Unit = {
          phaser.arriveAndAwaitAdvance()
          var i = 0
          while (i < 100) {
            try { res0.add(partitioner.partition("foo", null, null, null, sf0, cluster)) } catch {
              case _: Throwable => errors.incrementAndGet()
            }
            i += 1
          }
          phaser.arriveAndDeregister()
        }
      })
      CachedThreadPool.execute(new Runnable() {
        override def run(): Unit = {
          phaser.arriveAndAwaitAdvance()
          var i = 0
          while (i < 100) {
            try { res1.add(partitioner.partition("foo", null, null, null, sf1, cluster)) } catch {
              case _: Throwable => errors.incrementAndGet()
            }
            i += 1
          }
          phaser.arriveAndDeregister()
        }
      })

      phaser.arriveAndAwaitAdvance()
      phaser.arriveAndAwaitAdvance()

      errors.get mustEqual 0
      res0 mustEqual Collections.singleton(0)
      res1 mustEqual Collections.singleton(1)
    }
  }
}
