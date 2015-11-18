/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatSerializationTest extends Specification {

  sequential

  "StatsSerlization" should {
    "pack and unpack" >> {
      // Setup MinMax
      val attribute = "foo"
      val mm = new MinMax[java.lang.Long](attribute)

      val min = -235L
      val max = 12345L

      mm.min = min
      mm.max = max

      // Setup ISC.
      val isc = new IteratorStackCounter
      val count = 987654321L
      isc.count = count

      "MinMax stats" in {
        val packed   = StatSerialization.pack(mm)
        val unpacked = StatSerialization.unpack(packed).asInstanceOf[MinMax[java.lang.Long]]

        unpacked.attribute must be equalTo attribute
        unpacked.min must be equalTo min
        unpacked.max must be equalTo max
      }

      "IteratorStackCounter stats" in {
        val packed = StatSerialization.pack(isc)
        val unpacked = StatSerialization.unpack(packed).asInstanceOf[IteratorStackCounter]

        unpacked.count must be equalTo count
      }

      //TODO: Fill this in.
      "EnumeratedHistogram stats" in {
        true must be equalTo true
      }

      "Sequences of stats" in {

        val stats = new SeqStat(Seq(mm, isc))

        val packed = StatSerialization.pack(stats)
        val unpacked = StatSerialization.unpack(packed)

        unpacked must anInstanceOf[SeqStat]

        val seqs = unpacked.asInstanceOf[SeqStat].stats

        seqs(0) mustEqual mm
        seqs(1) mustEqual isc
      }
    }
  }
}
