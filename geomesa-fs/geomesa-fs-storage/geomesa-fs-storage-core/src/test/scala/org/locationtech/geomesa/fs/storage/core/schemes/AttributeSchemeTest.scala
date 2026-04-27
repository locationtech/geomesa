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
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

class AttributeSchemeTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,time:Long,weight:Float,precision:Double")

  "AttributeScheme" should {

    "partition by string attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "TestValue", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "testvalue"

      val sf2 = ScalaSimpleFeature.create(sft, "2", "AnotherTest", 20, 2000000L, 5.5f, 12.34d)
      ps.getPartition(sf2).value mustEqual "anothertest"
    }

    "partition by string attribute with width option" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name:width=4")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "TestValue", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "test"

      val sf2 = ScalaSimpleFeature.create(sft, "2", "AnotherTest", 20, 2000000L, 5.5f, 12.34d)
      ps.getPartition(sf2).value mustEqual "anot"

      val sf3 = ScalaSimpleFeature.create(sft, "3", "abc", 30, 3000000L, 7.8f, 15.67d)
      ps.getPartition(sf3).value mustEqual "abc"
    }

    "handle null string values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name")

      val sf = ScalaSimpleFeature.create(sft, "1", null, 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf).value mustEqual ""
    }

    "handle null string values with default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name:default=unknown")

      val sf = ScalaSimpleFeature.create(sft, "1", null, 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf).value mustEqual "unknown"
    }

    "partition by int attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual AttributeIndexKey.typeEncode(10)

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 20, 3000000L, 7.8f, 15.67d)
      ps.getPartition(sf2).value mustEqual AttributeIndexKey.typeEncode(20)
    }

    "partition by int attribute with divisor option" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age:divisor=10")

      foreach(Range(10, 20)) { i =>
        val sf = ScalaSimpleFeature.create(sft, "1", "test", i, 1000000L, 3.2f, 9.99d)
        ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(10)
      }

      foreach(Range(20, 30)) { i =>
        val sf = ScalaSimpleFeature.create(sft, "1", "test", i, 1000000L, 3.2f, 9.99d)
        ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(20)
      }
    }

    "handle null int values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", null, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(0)
    }

    "handle null int values with default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age:default=1")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", null, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(1)
    }

    "partition by long attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual AttributeIndexKey.typeEncode(1000000L)

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 2000000L, 7.8f, 15.67d)
      ps.getPartition(sf2).value mustEqual AttributeIndexKey.typeEncode(2000000L)
    }

    "partition by long attribute with divisor option" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time:divisor=100000")

      // 1050000 and 1080000 should bucket to 1000000
      foreach(Seq(1050000L, 1080000L)) { i =>
        val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, i, 3.2f, 9.99d)
        ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(1000000L)
      }

      // 1150000 should bucket to 1100000
      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1150000L, 7.8f, 15.67d)
      ps.getPartition(sf2).value mustEqual AttributeIndexKey.typeEncode(1100000L)
    }

    "handle null long values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, null, 3.2f, 9.99d)
      ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(0L)
    }

    "handle null long values with default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time:default=1")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, null, 3.2f, 9.99d)
      ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(1L)
    }

    "partition by float attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual AttributeIndexKey.typeEncode(3.2f)

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 5.5f, 15.67d)
      ps.getPartition(sf2).value mustEqual AttributeIndexKey.typeEncode(5.5f)
    }

    "partition by float attribute with scale option" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight:scale=1")

      // 3.25 and 3.28 should bucket to 3.2 (floor(3.25*10)/10 = 32/10 = 3.2)
      foreach(Seq(3.25f, 3.28f)) { i =>
        val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, i, 9.99d)
        ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(3.2f)
      }

      // 3.35 should bucket to 3.3
      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 3.35f, 15.67d)
      ps.getPartition(sf2).value mustEqual AttributeIndexKey.typeEncode(3.3f)
    }

    "handle null float values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, null, 9.99d)
      ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(0f)
    }

    "handle null float values with default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight:default=1")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, null, 9.99d)
      ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(1f)
    }

    "partition by double attribute" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual AttributeIndexKey.typeEncode(9.99d)

      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 3.2f, 12.34d)
      ps.getPartition(sf2).value mustEqual AttributeIndexKey.typeEncode(12.34d)
    }

    "partition by double attribute with scale option" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision:scale=2")

      // 9.991 and 9.995 should bucket to 9.99 (floor(9.991*100)/100 = 999/100 = 9.99)
      foreach(Seq(9.991d, 9.995d)) { i =>
        val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, i)
        ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(9.99d)
      }

      // 10.001 should bucket to 10.00
      val sf2 = ScalaSimpleFeature.create(sft, "3", "test", 10, 1000000L, 3.2f, 10.001d)
      ps.getPartition(sf2).value mustEqual AttributeIndexKey.typeEncode(10d)
    }

    "handle null double values with implicit default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, null)
      ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(0d)
    }

    "handle null double values with default" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision:default=1.0")

      val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, null)
      ps.getPartition(sf).value mustEqual AttributeIndexKey.typeEncode(1d)
    }

    "filter string values to allowed partitions" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name:allow=test:allow=another")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "Test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual "test"

      val sf2 = ScalaSimpleFeature.create(sft, "2", "another", 20, 2000000L, 5.5f, 12.34d)
      ps.getPartition(sf2).value mustEqual "another"

      // value not in allowed list should get default partition
      val sf3 = ScalaSimpleFeature.create(sft, "3", "forbidden", 30, 3000000L, 7.8f, 15.67d)
      ps.getPartition(sf3).value mustEqual ""
    }

    "filter int values to allowed partitions" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age:allow=10:allow=20:default=10")

      val sf1 = ScalaSimpleFeature.create(sft, "1", "test", 10, 1000000L, 3.2f, 9.99d)
      ps.getPartition(sf1).value mustEqual AttributeIndexKey.typeEncode(10)

      val sf2 = ScalaSimpleFeature.create(sft, "2", "test", 20, 2000000L, 5.5f, 12.34d)
      ps.getPartition(sf2).value mustEqual AttributeIndexKey.typeEncode(20)

      // value not in allowed list should get default partition
      val sf3 = ScalaSimpleFeature.create(sft, "3", "test", 15, 3000000L, 7.8f, 15.67d)
      ps.getPartition(sf3).value mustEqual AttributeIndexKey.typeEncode(10)
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

      // validate filter structure - should be an ILIKE filter matching the exact string
      filter mustEqual ECQL.toFilter("name ILIKE 'testvalue'")
      // note: like filter equality doesn't validate case matching, so we check it here explicitly
      filter.asInstanceOf[PropertyIsLike].isMatchingCase must beFalse

      // the filter should match the feature
      filter.evaluate(sf1) must beTrue
    }

    "calculate covering filter for string partition with width" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name:width=4")
      val sf1 = ScalaSimpleFeature.create(sft, "1", "TestValue", 10, 1000000L, 3.2f, 9.99d)
      val partition = ps.getPartition(sf1)

      val filter = ps.getCoveringFilter(partition)
      filter must not(beNull)

      // validate filter structure - should be an ILIKE filter with wildcard for prefix matching
      filter mustEqual ECQL.toFilter("name ILIKE 'test%'")
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

    // getIntersectingPartitions tests

    "calculate intersecting partitions for string equality filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name")
      val filter = ECQL.toFilter("name = 'Testvalue'")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      partitions.get.head.lower mustEqual "testvalue"
      partitions.get.head.upper mustEqual "testvalue" + ZeroChar
    }

    "calculate intersecting partitions for string IN filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name")
      val filter = ECQL.toFilter("name IN ('test', 'another')")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(2)

      partitions.get must contain(PartitionRange(ps.name, "test", "test" + ZeroChar))
      partitions.get must contain(PartitionRange(ps.name, "another", "another" + ZeroChar))
    }

    "calculate intersecting partitions for string range filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name")
      val filter = ECQL.toFilter("name >= 'a' AND name < 'z'")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      partitions.get.head.name mustEqual ps.name
      partitions.get.head.lower mustEqual "a"
      partitions.get.head.upper mustEqual "z"
    }

    "calculate intersecting partitions for string range filter with allowed values" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name:allow=test:allow=another")
      val filter = ECQL.toFilter("name >= 'a' AND name < 'z'")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(2)
      partitions.get must contain(PartitionRange(ps.name, "test", "test" + ZeroChar))
      partitions.get must contain(PartitionRange(ps.name, "another", "another" + ZeroChar))
    }

    "calculate intersecting partitions for int equality filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age")
      val filter = ECQL.toFilter("age = 10")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      partitions.get.head.lower mustEqual AttributeIndexKey.typeEncode(10)
      partitions.get.head.upper mustEqual AttributeIndexKey.typeEncode(10) + ZeroChar
    }

    "calculate intersecting partitions for int range filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age")
      val filter = ECQL.toFilter("age >= 10 AND age <= 20")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      partitions.get.head.name mustEqual ps.name
      partitions.get.head.lower mustEqual AttributeIndexKey.typeEncode(10)
      partitions.get.head.upper mustEqual AttributeIndexKey.typeEncode(20) + ZeroChar
    }

    "calculate intersecting partitions for int with divisor" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=age:divisor=10")
      val filter = ECQL.toFilter("age >= 15 AND age < 25")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      val expected = PartitionRange(ps.name, AttributeIndexKey.typeEncode(10), AttributeIndexKey.typeEncode(20) + ZeroChar)
      partitions.get.head mustEqual expected
    }

    "calculate intersecting partitions for long equality filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time")
      val filter = ECQL.toFilter("time = 1000000")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      val expected = PartitionRange(ps.name, AttributeIndexKey.typeEncode(1000000L), AttributeIndexKey.typeEncode(1000000L) + ZeroChar)
      partitions.get.head mustEqual expected
    }

    "calculate intersecting partitions for long range filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time")
      val filter = ECQL.toFilter("time >= 1000000 AND time < 2000000")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      partitions.get.head mustEqual PartitionRange(ps.name, AttributeIndexKey.typeEncode(1000000L), AttributeIndexKey.typeEncode(2000000L))
    }

    "calculate intersecting partitions for long with divisor" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=time:divisor=100000")
      val filter = ECQL.toFilter("time >= 1050000 AND time < 1150000")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      val expected = PartitionRange(ps.name, AttributeIndexKey.typeEncode(1000000L), AttributeIndexKey.typeEncode(1100000L) + ZeroChar)
      partitions.get.head mustEqual expected
    }

    "calculate intersecting partitions for float equality filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight")
      val filter = ECQL.toFilter("weight = 3.2")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      val expected = PartitionRange(ps.name, AttributeIndexKey.typeEncode(3.2f), AttributeIndexKey.typeEncode(3.2f) + ZeroChar)
      partitions.get.head mustEqual expected
    }

    "calculate intersecting partitions for float range filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight")
      val filter = ECQL.toFilter("weight >= 3.0 AND weight < 5.0")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      val expected = PartitionRange(ps.name, AttributeIndexKey.typeEncode(3f), AttributeIndexKey.typeEncode(5f))
      partitions.get.head mustEqual expected
    }

    "calculate intersecting partitions for float with scale" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=weight:scale=1")
      val filter = ECQL.toFilter("weight >= 3.22 AND weight < 3.45")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      val expected = PartitionRange(ps.name, AttributeIndexKey.typeEncode(3.2f), AttributeIndexKey.typeEncode(3.4f) + ZeroChar)
      partitions.get.head mustEqual expected
    }

    "calculate intersecting partitions for double equality filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision")
      val filter = ECQL.toFilter("precision = 9.99")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      val expected = PartitionRange(ps.name, AttributeIndexKey.typeEncode(9.99), AttributeIndexKey.typeEncode(9.99) + ZeroChar)
      partitions.get.head mustEqual expected
    }

    "calculate intersecting partitions for double range filter" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision")
      val filter = ECQL.toFilter("precision >= 9.0 AND precision < 11.0")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      val expected = PartitionRange(ps.name, AttributeIndexKey.typeEncode(9d), AttributeIndexKey.typeEncode(11d))
      partitions.get.head mustEqual expected
    }

    "calculate intersecting partitions for double with scale" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=precision:scale=2")
      val filter = ECQL.toFilter("precision >= 9.999 AND precision < 10.011")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(1)
      val expected = PartitionRange(ps.name, AttributeIndexKey.typeEncode(9.99d), AttributeIndexKey.typeEncode(10.01d) + ZeroChar)
      partitions.get.head mustEqual expected
    }

    "return None for filters on unrelated attributes" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name")
      val filter = ECQL.toFilter("age = 10")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beNone
    }

    "return empty partitions for disjoint filters" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name")
      val filter = ECQL.toFilter("name = 'test' AND name = 'other'")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must beEmpty
    }

    "handle allowed values with intersecting partitions" in {
      val ps = PartitionSchemeFactory.load(sft, "attribute:attribute=name:allow=test:allow=another")
      val filter = ECQL.toFilter("name IN ('test', 'another', 'forbidden')")

      val partitions = ps.getRangesForFilter(filter)
      partitions must beSome
      partitions.get must haveLength(2)

      // only the allowed values should be returned
      partitions.get must contain(PartitionRange(ps.name, "test", "test" + ZeroChar))
      partitions.get must contain(PartitionRange(ps.name, "another", "another" + ZeroChar))
    }
  }
}
