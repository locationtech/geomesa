/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

import java.util.Random

class XZ2SchemeTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createType("test", "*geom:Point:srid=4326")

  "XZ2Scheme" should {

    "partition based on 12 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "xz2:bits=12")
      ps must beAnInstanceOf[XZ2Scheme]
      ps.asInstanceOf[XZ2Scheme].bits mustEqual 12

      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (10 10)")).value mustEqual "807e"
      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (-75 38)")).value mustEqual "66f2"
    }

    "partition based on 20 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "xz2:bits=20")
      ps must beAnInstanceOf[XZ2Scheme]
      ps.asInstanceOf[XZ2Scheme].bits mustEqual 20

      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (10 10)")).value mustEqual "807e0a"
      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (-75 38)")).value mustEqual "66f2db"
    }

    "calculate covering filters" in {
      val r = new Random(77)
      val features = Seq.tabulate(100) { i =>
        ScalaSimpleFeature.create(sft, s"$i", s"POINT (${r.nextDouble(-180, 180)} ${r.nextDouble(-90, 90)})")
      }

      foreach(Seq(4, 8, 16)) { bits =>
        val ps = PartitionSchemeFactory.load(sft, s"xz2:bits=$bits")
        ps must beAnInstanceOf[XZ2Scheme]
        foreach(features.groupBy(ps.getPartition)) { case (partition, group) =>
          val filter = ps.getCoveringFilter(partition)
          features.filter(filter.evaluate) must containTheSameElementsAs(group)
        }
      }
    }
  }
}
