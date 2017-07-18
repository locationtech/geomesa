/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ImmutableSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SharedStateTest extends Specification with LazyLogging {

  val sft = SimpleFeatureTypes.createType("ss", "name:String")

  val one = new ImmutableSimpleFeature(sft, "1", Array("one"))
  val two = new ImmutableSimpleFeature(sft, "2", Array("two"))
  val three = new ImmutableSimpleFeature(sft, "3", Array("three"))

  "SharedState" should {
    "expire features directly" >> {
      val ss = SharedState("")
      ss.partitionAssigned(1, -1L)
      ss.partitionAssigned(0, -1L)
      ss.add(one, 0, 0, 0)
      ss.add(two, 1, 0, 1)
      ss.add(three, 0, 1, 2)
      ss.all().toSeq must containTheSameElementsAs(Seq(one, two, three))
      ss.get("1") mustEqual one
      ss.get("2") mustEqual two
      ss.get("3") mustEqual three
      ss.expired(0) must beEmpty
      ss.expired(1) mustEqual Seq(0)
      ss.expired(4) mustEqual Seq(0, 1)
      ss.expired(0, 1) mustEqual (0, Seq((0, one)))
      ss.all().toSeq must containTheSameElementsAs(Seq(two, three))
      ss.expired(1, 2) mustEqual (0, Seq((0, two)))
      ss.all().toSeq mustEqual Seq(three)
      ss.expired(0, 4) mustEqual (1, Seq((1, three)))
      ss.all() must beEmpty
    }
    "expire features indirectly" >> {
      val ss = SharedState("")
      ss.partitionAssigned(1, -1L)
      ss.partitionAssigned(0, -1L)
      ss.add(one, 0, 0, 0)
      ss.add(two, 1, 0, 1)
      ss.add(three, 0, 1, 2)
      ss.all().toSeq must containTheSameElementsAs(Seq(one, two, three))
      ss.get("1") mustEqual one
      ss.get("2") mustEqual two
      ss.get("3") mustEqual three
      ss.offsetChanged(0, 0)
      ss.all().toSeq must containTheSameElementsAs(Seq(two, three))
      ss.offsetChanged(1, 0)
      ss.all().toSeq mustEqual Seq(three)
      ss.offsetChanged(0, 1)
      ss.all() must beEmpty
    }
  }
}
