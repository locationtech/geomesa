/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

class HashSchemeTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,time:Long,weight:Float,precision:Double")

  "HashScheme" should {

    "partition by string attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "hash:buckets=8:attribute=name")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "TestValue", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "4"

      val sf2 = ScalaSimpleFeature.create(sft, "2", "AnotherTest", 20, 2000000L, 5.5f, 12.34d)
      ps.getPartition(sf2).value mustEqual "0"
    }

    "partition by int attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "hash:buckets=8:attribute=age")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "4"

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 20, 3000000L, 7.8f, 15.67d)
      ps.getPartition(sf2).value mustEqual "3"
    }

    "partition by long attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "hash:buckets=8:attribute=time")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "6"

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 2000000L, 7.8f, 15.67d)
      ps.getPartition(sf2).value mustEqual "3"
    }

    "partition by float attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "hash:buckets=8:attribute=weight")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "2"

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 5.5f, 15.67d)
      ps.getPartition(sf2).value mustEqual "1"
    }

    "partition by double attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "hash:buckets=8:attribute=precision")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "2"

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 3.2f, 12.34d)
      ps.getPartition(sf2).value mustEqual "7"
    }

    "handle null values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "hash:buckets=8:attribute=name")

      val sf = ScalaSimpleFeature.create(sft, "1", null, 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf).value mustEqual "0"
    }

    "pad partitions correctly" in {
      val ps = PartitionSchemeFactory.load(sft, "hash:buckets=16:attribute=name")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "TestValue", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "12"

      val sf2 = ScalaSimpleFeature.create(sft, "2", "AnotherTest", 20, 2000000L, 5.5f, 12.34d)
      ps.getPartition(sf2).value mustEqual "08"
    }

    "calculate covering filters" in {
      val ps = PartitionSchemeFactory.load(sft, "hash:buckets=8:attribute=name")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "TestValue", 10, 1000000L, 3.2f, 9.99d)

      val partition = ps.getPartition(sf1)
      partition.value mustEqual "4"
      val filter = ps.getCoveringFilter(partition)
      filter mustEqual ECQL.toFilter("bucketHash(name,8) = '4'")
      // the filter should match the feature
      filter.evaluate(sf1) must beTrue
    }
  }
}
