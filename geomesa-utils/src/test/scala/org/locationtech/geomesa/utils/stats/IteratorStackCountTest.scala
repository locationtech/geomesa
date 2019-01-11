/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IteratorStackCountTest extends Specification with StatTestHelper {
  sequential

  "IteratorStackCount stat" should {
    "create an IteratorStackCount" in {
      val stat = Stat(sft, "IteratorStackCount()")
      val isc = stat.asInstanceOf[IteratorStackCount]

      isc.counter mustEqual 1L
      isc.isEmpty must beFalse

      "serialize and deserialize" in {
        val packed   = StatSerializer(sft).serialize(isc)
        val unpacked = StatSerializer(sft).deserialize(packed).asInstanceOf[IteratorStackCount]

        unpacked.toJson mustEqual isc.toJson
      }


      "deserialize as immutable value" >> {
        val packed   = StatSerializer(sft).serialize(isc)
        val unpacked = StatSerializer(sft).deserialize(packed, immutable = true).asInstanceOf[IteratorStackCount]

        unpacked.toJson mustEqual isc.toJson

        unpacked.clear must throwAn[Exception]
        unpacked.+=(stat) must throwAn[Exception]
        unpacked.observe(features.head) must throwAn[Exception]
        unpacked.unobserve(features.head) must throwAn[Exception]
      }

      "combine two IteratorStackCounts" in {
        val stat2 = Stat(sft, "IteratorStackCount()")
        val isc2 = stat2.asInstanceOf[IteratorStackCount]
        isc2.counter = 5L

        isc += isc2

        isc.counter mustEqual 6L
        isc2.counter mustEqual 5L

        "clear them" in {
          isc.isEmpty must beFalse
          isc2.isEmpty must beFalse

          isc.clear()
          isc2.clear()

          isc.counter mustEqual 1L
          isc2.counter mustEqual 1L
        }
      }
    }
  }
}
