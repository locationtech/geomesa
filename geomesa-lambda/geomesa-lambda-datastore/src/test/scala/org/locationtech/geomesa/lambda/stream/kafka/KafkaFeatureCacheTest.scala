/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature.ImmutableSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.lambda.LambdaContainerTest.TestClock
import org.locationtech.geomesa.lambda.data.LambdaDataStore.PersistenceConfig
import org.locationtech.geomesa.lambda.stream.OffsetManager
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache.PartitionOffset
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.Closeable
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class KafkaFeatureCacheTest extends Specification with Mockito {

  lazy val sft = SimpleFeatureTypes.createType("ss", "name:String")

  lazy val one = new ImmutableSimpleFeature(sft, "1", Array("one"))
  lazy val two = new ImmutableSimpleFeature(sft, "2", Array("two"))
  lazy val three = new ImmutableSimpleFeature(sft, "3", Array("three"))

  "KafkaFeatureCache" should {
    "expire features actively" in {
      implicit val clock: TestClock = new TestClock()
      val manager = mock[OffsetManager]
      manager.acquireLock(anyString, anyInt, anyLong) returns Some(mock[Closeable])
      WithClose(new TestGeoMesaDataStore(looseBBox = true)) { ds =>
        ds.createSchema(sft)
        WithClose(new KafkaFeatureCache(ds, sft, manager, "", Some(PersistenceConfig(Duration(1, "ms"), 100)))) { cache =>
          cache.partitionAssigned(1, -1L)
          cache.partitionAssigned(0, -1L)
          cache.add(one, 0, 0, 0)
          cache.add(two, 1, 0, 1)
          cache.add(three, 0, 1, 2)
          cache.all().toSeq must containTheSameElementsAs(Seq(one, two, three))
          cache.get("1") mustEqual one
          cache.get("2") mustEqual two
          cache.get("3") mustEqual three

          manager.getOffset("", 0) returns -1L
          cache.persist() must beEmpty
          SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features()).toList must beEmpty
          clock.tick = 1
          manager.getOffset("", 0) returns -1L
          manager.getOffset("", 1) returns -1L
          cache.persist() mustEqual Seq(PartitionOffset(0, 0))
          there was one(manager).setOffset("", 0, 0)
          cache.offsetChanged(0, 0)
          cache.all().toSeq must containTheSameElementsAs(Seq(two, three))
          SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features()).toList mustEqual Seq(one)
          clock.tick = 4
          manager.getOffset("", 0) returns 0L
          manager.getOffset("", 1) returns -1L
          cache.persist() must containTheSameElementsAs(Seq(PartitionOffset(0, 1), PartitionOffset(1, 0)))
          there was one(manager).setOffset("", 0, 1)
          there was one(manager).setOffset("", 1, 0)
          cache.offsetChanged(0, 1)
          cache.offsetChanged(1, 0)
          cache.all() must beEmpty
          clock.tick = 4
          SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features()).toList must
            containTheSameElementsAs(Seq(one, two, three))
        }
      }
    }
    "expire features passively" in {
      implicit val clock: TestClock = new TestClock()
      val manager = mock[OffsetManager]
      WithClose(new KafkaFeatureCache(null, sft, manager, "", None)) { cache =>
        cache.partitionAssigned(1, -1L)
        cache.partitionAssigned(0, -1L)
        cache.add(one, 0, 0, 0)
        cache.add(two, 1, 0, 1)
        cache.add(three, 0, 1, 2)
        cache.all().toSeq must containTheSameElementsAs(Seq(one, two, three))
        cache.get("1") mustEqual one
        cache.get("2") mustEqual two
        cache.get("3") mustEqual three
        cache.offsetChanged(0, 0)
        cache.all().toSeq must containTheSameElementsAs(Seq(two, three))
        cache.offsetChanged(1, 0)
        cache.all().toSeq mustEqual Seq(three)
        cache.offsetChanged(0, 1)
        cache.all() must beEmpty
      }
    }
  }
}
