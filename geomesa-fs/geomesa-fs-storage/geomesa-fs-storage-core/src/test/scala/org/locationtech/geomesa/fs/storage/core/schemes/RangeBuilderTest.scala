/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.specs2.mutable.SpecificationWithJUnit

class RangeBuilderTest extends SpecificationWithJUnit {

  "RangeBuilder" should {

    "handle a single range" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-02")
      val result = builder.result()
      result mustEqual Seq(PartitionRange("test", "2020-01-01", "2020-01-02"))
    }

    "handle non-overlapping ranges added in order" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-02")
      builder += PartitionRange("test", "2020-01-03", "2020-01-04")
      builder += PartitionRange("test", "2020-01-05", "2020-01-06")
      val result = builder.result()
      result mustEqual Seq(
        PartitionRange("test", "2020-01-01", "2020-01-02"),
        PartitionRange("test", "2020-01-03", "2020-01-04"),
        PartitionRange("test", "2020-01-05", "2020-01-06")
      )
    }

    "handle non-overlapping ranges added in reverse order" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-05", "2020-01-06")
      builder += PartitionRange("test", "2020-01-03", "2020-01-04")
      builder += PartitionRange("test", "2020-01-01", "2020-01-02")
      val result = builder.result()
      result mustEqual Seq(
        PartitionRange("test", "2020-01-01", "2020-01-02"),
        PartitionRange("test", "2020-01-03", "2020-01-04"),
        PartitionRange("test", "2020-01-05", "2020-01-06")
      )
    }

    "merge overlapping ranges at the beginning" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-03")
      builder += PartitionRange("test", "2020-01-05", "2020-01-06")
      builder += PartitionRange("test", "2020-01-02", "2020-01-04")
      val result = builder.result()
      result mustEqual Seq(
        PartitionRange("test", "2020-01-01", "2020-01-04"),
        PartitionRange("test", "2020-01-05", "2020-01-06")
      )
    }

    "merge overlapping ranges at the end" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-02")
      builder += PartitionRange("test", "2020-01-05", "2020-01-07")
      builder += PartitionRange("test", "2020-01-06", "2020-01-08")
      val result = builder.result()
      result mustEqual Seq(
        PartitionRange("test", "2020-01-01", "2020-01-02"),
        PartitionRange("test", "2020-01-05", "2020-01-08")
      )
    }

    "merge overlapping ranges in the middle" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-02")
      builder += PartitionRange("test", "2020-01-03", "2020-01-05")
      builder += PartitionRange("test", "2020-01-07", "2020-01-08")
      builder += PartitionRange("test", "2020-01-04", "2020-01-06")
      val result = builder.result()
      result mustEqual Seq(
        PartitionRange("test", "2020-01-01", "2020-01-02"),
        PartitionRange("test", "2020-01-03", "2020-01-06"),
        PartitionRange("test", "2020-01-07", "2020-01-08")
      )
    }

    "merge adjacent ranges (upper bound equals lower bound)" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-03")
      builder += PartitionRange("test", "2020-01-03", "2020-01-05")
      val result = builder.result()
      result mustEqual Seq(PartitionRange("test", "2020-01-01", "2020-01-05"))
    }

    "merge multiple adjacent and overlapping ranges" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-02")
      builder += PartitionRange("test", "2020-01-02", "2020-01-03")
      builder += PartitionRange("test", "2020-01-03", "2020-01-04")
      builder += PartitionRange("test", "2020-01-04", "2020-01-05")
      val result = builder.result()
      result mustEqual Seq(PartitionRange("test", "2020-01-01", "2020-01-05"))
    }

    "merge when new range completely contains existing range" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-02", "2020-01-03")
      builder += PartitionRange("test", "2020-01-01", "2020-01-05")
      val result = builder.result()
      result mustEqual Seq(PartitionRange("test", "2020-01-01", "2020-01-05"))
    }

    "merge when existing range completely contains new range" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-05")
      builder += PartitionRange("test", "2020-01-02", "2020-01-03")
      val result = builder.result()
      result mustEqual Seq(PartitionRange("test", "2020-01-01", "2020-01-05"))
    }

    "merge ranges with partial overlap" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-04")
      builder += PartitionRange("test", "2020-01-03", "2020-01-06")
      val result = builder.result()
      result mustEqual Seq(PartitionRange("test", "2020-01-01", "2020-01-06"))
    }

    "merge ranges added in random order" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-07", "2020-01-08")
      builder += PartitionRange("test", "2020-01-03", "2020-01-05")
      builder += PartitionRange("test", "2020-01-01", "2020-01-02")
      builder += PartitionRange("test", "2020-01-04", "2020-01-06")
      builder += PartitionRange("test", "2020-01-06", "2020-01-09")
      val result = builder.result()
      result mustEqual Seq(
        PartitionRange("test", "2020-01-01", "2020-01-02"),
        PartitionRange("test", "2020-01-03", "2020-01-09")
      )
    }

    "handle complex merging scenario with multiple ranges" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-02")
      builder += PartitionRange("test", "2020-01-05", "2020-01-06")
      builder += PartitionRange("test", "2020-01-10", "2020-01-11")
      builder += PartitionRange("test", "2020-01-15", "2020-01-16")
      // add a range that doesn't merge with anything
      builder += PartitionRange("test", "2020-01-20", "2020-01-21")
      // add a range that merges two separate ranges
      builder += PartitionRange("test", "2020-01-01", "2020-01-06")
      val result = builder.result()
      result mustEqual Seq(
        PartitionRange("test", "2020-01-01", "2020-01-06"),
        PartitionRange("test", "2020-01-10", "2020-01-11"),
        PartitionRange("test", "2020-01-15", "2020-01-16"),
        PartitionRange("test", "2020-01-20", "2020-01-21")
      )
    }

    "handle identical ranges" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-01", "2020-01-02")
      builder += PartitionRange("test", "2020-01-01", "2020-01-02")
      val result = builder.result()
      result mustEqual Seq(PartitionRange("test", "2020-01-01", "2020-01-02"))
    }

    "preserve order after multiple inserts and merges" in {
      val builder = new RangeBuilder()
      builder += PartitionRange("test", "2020-01-10", "2020-01-15")
      builder += PartitionRange("test", "2020-01-01", "2020-01-05")
      builder += PartitionRange("test", "2020-01-20", "2020-01-25")
      builder += PartitionRange("test", "2020-01-03", "2020-01-12")
      val result = builder.result()
      result must haveSize(2)
      result mustEqual Seq(
        PartitionRange("test", "2020-01-01", "2020-01-15"),
        PartitionRange("test", "2020-01-20", "2020-01-25")
      )
    }
  }
}
