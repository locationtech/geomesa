/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.geotools.api.filter.PropertyIsLike
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.decomposeAnd
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

class AttributeSchemeTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,time:Long,weight:Float,precision:Double")

  "AttributeScheme" should {

    "partition by string attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "TestValue", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "TestValue"

      val sf2 = ScalaSimpleFeature.create(sft, "2", "AnotherTest", 20, 2000000L, 5.5f, 12.34d)
      ps.getPartition(sf2).value mustEqual "AnotherTest"
    }

    "partition by string attribute with width option" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name:width=4")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "TestValue", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "Test"

      val sf2 = ScalaSimpleFeature.create(sft, "2", "AnotherTest", 20, 2000000L, 5.5f, 12.34d)
      ps.getPartition(sf2).value mustEqual "Anot"

      val sf3 = ScalaSimpleFeature.create(sft, "3", "abc", 30, 3000000L, 7.8f, 15.67d)
      ps.getPartition(sf3).value mustEqual "abc"
    }

    "handle null string values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name")

      val sf = ScalaSimpleFeature.create(sft, "1", null, 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf).value mustEqual ""
    }

    "partition by int attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "10"

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 20, 3000000L, 7.8f, 15.67d)
      ps.getPartition(sf2).value mustEqual "20"
    }

    "partition by int attribute with divisor option" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age:divisor=10")

      foreach(Range(10, 20)) { i =>
        val sf = ScalaSimpleFeature.create(sft, "1", "test", i, 1000000L, 3.2f, 9.99d)
        ps.getPartition(sf).value mustEqual "10"
      }

      foreach(Range(20, 30)) { i =>
        val sf = ScalaSimpleFeature.create(sft, "1", "test", i, 1000000L, 3.2f, 9.99d)
        ps.getPartition(sf).value mustEqual "20"
      }
    }

    "handle null int values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", null, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf).value mustEqual "0"
    }

    "partition by long attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "1000000"

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 2000000L, 7.8f, 15.67d)
      ps.getPartition(sf2).value mustEqual "2000000"
    }

    "partition by long attribute with divisor option" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time:divisor=100000")

      // 1050000 and 1080000 should bucket to 1000000
      foreach(Seq(1050000L, 1080000L)) { i =>
        val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, i, 3.2f, 9.99d)
        ps.getPartition(sf).value mustEqual "1000000"
      }

      // 1150000 should bucket to 1100000
      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1150000L, 7.8f, 15.67d)
      ps.getPartition(sf2).value mustEqual "1100000"
    }

    "handle null long values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, null, 3.2f, 9.99d)
      ps.getPartition(sf).value mustEqual "0"
    }

    "partition by float attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "3.2"

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 5.5f, 15.67d)
      ps.getPartition(sf2).value mustEqual "5.5"
    }

    "partition by float attribute with scale option" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight:scale=1")

      // 3.25 and 3.28 should bucket to 3.2 (floor(3.25*10)/10 = 32/10 = 3.2)
      foreach(Seq(3.25f, 3.28f)) { i =>
        val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, i, 9.99d)
        ps.getPartition(sf).value mustEqual "3.2"
      }

      // 3.35 should bucket to 3.3
      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 3.35f, 15.67d)
      ps.getPartition(sf2).value mustEqual "3.3"
    }

    "handle null float values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, null, 9.99d)
      ps.getPartition(sf).value mustEqual "0.0"
    }

    "partition by double attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "9.99"

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 3.2f, 12.34d)
      ps.getPartition(sf2).value mustEqual "12.34"
    }

    "partition by double attribute with scale option" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision:scale=2")

      // 9.991 and 9.995 should bucket to 9.99 (floor(9.991*100)/100 = 999/100 = 9.99)
      foreach(Seq(9.991d, 9.995d)) { i =>
        val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, i)
        ps.getPartition(sf).value mustEqual "9.99"
      }

      // 10.001 should bucket to 10.00
      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 3.2f, 10.001d)
      ps.getPartition(sf2).value mustEqual "10.0"
    }

    "handle null double values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, null)
      ps.getPartition(sf).value mustEqual "0.0"
    }

    "reject invalid option combinations" in {
      // width option on non-string type
      PartitionSchemeFactory.load(sft, "attribute:attribute=age:width=4") must throwA[IllegalArgumentException]

      // divisor option on non-integer type
      PartitionSchemeFactory.load(sft, "attribute:attribute=name:divisor=10") must throwA[IllegalArgumentException]

      // scale option on non-decimal type
      PartitionSchemeFactory.load(sft, "attribute:attribute=age:scale=1") must throwA[IllegalArgumentException]
    }

    // getCoveringFilter tests

    "calculate covering filter for string partition" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "TestValue", 10, 1000000L, 3.2f, 9.99d)

      val filter = ps.getCoveringFilter(ps.getPartition(sf1))
      filter mustEqual ECQL.toFilter("name = 'TestValue'")

      // the filter should match the feature
      filter.evaluate(sf1) must beTrue
    }

    "calculate covering filter for string partition with width" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name:width=4")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "TestValue", 10, 1000000L, 3.2f, 9.99d)
      val partition = ps.getPartition(sf1)

      val filter = ps.getCoveringFilter(partition)
      filter must not(beNull)

      // validate filter structure - should be an LIKE filter with wildcard for prefix matching
      filter mustEqual ECQL.toFilter("name LIKE 'Test%'")
      // note: like filter equality doesn't validate case matching, so we check it here explicitly
      filter.asInstanceOf[PropertyIsLike].isMatchingCase must beFalse

      // the filter should match features with the same prefix
      filter.evaluate(sf1) must beTrue

      val sf2 = ScalaSimpleFeature.create(sft, "2", "TestAnother", 20, 2000000L, 5.5f, 12.34d)
      filter.evaluate(sf2) must beTrue

      val sf3 = ScalaSimpleFeature.create(sft, "3", "Other", 30, 3000000L, 7.8f, 15.67d)
      filter.evaluate(sf3) must beFalse
    }

    "calculate covering filter for int partition" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      val partition = ps.getPartition(sf1)

      val filter = ps.getCoveringFilter(partition)
      filter mustEqual ECQL.toFilter("age = 10")

      // the filter should match the feature
      filter.evaluate(sf1) must beTrue
    }

    "calculate covering filter for int partition with divisor" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age:divisor=10")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 15, 1000000L, 3.2f, 9.99d)
      val partition = ps.getPartition(sf1)

      val filter = ps.getCoveringFilter(partition)

      // validate filter structure - should be a range filter
      val expected = ECQL.toFilter("age >= 10 AND age < 20")
      decomposeAnd(filter) must containTheSameElementsAs(decomposeAnd(expected))

      // the filter should match features in the same bucket (10-19)
      filter.evaluate(sf1) must beTrue

      val sf2 = ScalaSimpleFeature.create(sft, "2", "test", 18, 2000000L, 5.5f, 12.34d)
      filter.evaluate(sf2) must beTrue

      val sf3 = ScalaSimpleFeature.create(sft, "3", "test", 25, 3000000L, 7.8f, 15.67d)
      filter.evaluate(sf3) must beFalse
    }

    "calculate covering filter for long partition" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      val partition = ps.getPartition(sf1)

      val filter = ps.getCoveringFilter(partition)
      filter mustEqual ECQL.toFilter("time = 1000000")

      // the filter should match the feature
      filter.evaluate(sf1) must beTrue
    }

    "calculate covering filter for long partition with divisor" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time:divisor=100000")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1050000L, 3.2f, 9.99d)
      val partition = ps.getPartition(sf1)

      val filter = ps.getCoveringFilter(partition)

      // validate filter structure - should be a range filter
      val expected = ECQL.toFilter("time >= 1000000 AND time < 1100000")
      decomposeAnd(filter) must containTheSameElementsAs(decomposeAnd(expected))

      // the filter should match features in the same bucket (1000000-1099999)
      filter.evaluate(sf1) must beTrue

      val sf2 = ScalaSimpleFeature.create(sft, "2", "test", 10, 1080000L, 5.5f, 12.34d)
      filter.evaluate(sf2) must beTrue

      val sf3 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1150000L, 7.8f, 15.67d)
      filter.evaluate(sf3) must beFalse
    }

    "calculate covering filter for float partition" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      val partition = ps.getPartition(sf1)

      val filter = ps.getCoveringFilter(partition)
      filter mustEqual ECQL.toFilter("weight = 3.2")

      // the filter should match the feature
      filter.evaluate(sf1) must beTrue
    }

    "calculate covering filter for float partition with scale" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight:scale=1")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.25f, 9.99d)
      val partition = ps.getPartition(sf1)

      val filter = ps.getCoveringFilter(partition)

      // validate filter structure - should be a range filter
      val expected = ECQL.toFilter("weight >= 3.2 AND weight < 3.3")
      decomposeAnd(filter) must containTheSameElementsAs(decomposeAnd(expected))

      // the filter should match features in the same bucket (3.2-3.3)
      filter.evaluate(sf1) must beTrue

      val sf2 = ScalaSimpleFeature.create(sft, "2", "test", 10, 1000000L, 3.28f, 12.34d)
      filter.evaluate(sf2) must beTrue

      val sf3 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 3.35f, 15.67d)
      filter.evaluate(sf3) must beFalse
    }

    "calculate covering filter for double partition" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      val partition = ps.getPartition(sf1)

      val filter = ps.getCoveringFilter(partition)
      filter mustEqual ECQL.toFilter("precision = 9.99")

      // the filter should match the feature
      filter.evaluate(sf1) must beTrue
    }

    "calculate covering filter for double partition with scale" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision:scale=2")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.991d)
      val partition = ps.getPartition(sf1)

      val filter = ps.getCoveringFilter(partition)

      // validate filter structure - should be a range filter
      val expected = ECQL.toFilter("precision >= 9.99 AND precision < 10.0")
      decomposeAnd(filter) must containTheSameElementsAs(decomposeAnd(expected))

      // the filter should match features in the same bucket (9.99-10.00)
      filter.evaluate(sf1) must beTrue

      val sf2 = ScalaSimpleFeature.create(sft, "2", "test", 10, 1000000L, 3.2f, 9.995d)
      filter.evaluate(sf2) must beTrue

      val sf3 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 3.2f, 10.01d)
      filter.evaluate(sf3) must beFalse
    }
  }
}
