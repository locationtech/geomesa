/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature.ImmutableSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KafkaFeatureCacheTest extends Specification with LazyLogging {

  val sft = SimpleFeatureTypes.createType("ss", "name:String")

  val one = new ImmutableSimpleFeature(sft, "1", Array("one"))
  val two = new ImmutableSimpleFeature(sft, "2", Array("two"))
  val three = new ImmutableSimpleFeature(sft, "3", Array("three"))

  "SharedState" should {
    "expire features directly" >> {
      val cache = new KafkaFeatureCache("")
      cache.partitionAssigned(1, -1L)
      cache.partitionAssigned(0, -1L)
      cache.add(one, 0, 0, 0)
      cache.add(two, 1, 0, 1)
      cache.add(three, 0, 1, 2)
      cache.all().toSeq must containTheSameElementsAs(Seq(one, two, three))
      cache.get("1") mustEqual one
      cache.get("2") mustEqual two
      cache.get("3") mustEqual three
      cache.expired(0) must beEmpty
      cache.expired(1) mustEqual Seq(0)
      cache.expired(4) mustEqual Seq(0, 1)
      cache.expired(0, 1) mustEqual (0, Seq((0, one)))
      cache.all().toSeq must containTheSameElementsAs(Seq(two, three))
      cache.expired(1, 2) mustEqual (0, Seq((0, two)))
      cache.all().toSeq mustEqual Seq(three)
      cache.expired(0, 4) mustEqual (1, Seq((1, three)))
      cache.all() must beEmpty
    }
    "expire features indirectly" >> {
      val cache = new KafkaFeatureCache("")
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
