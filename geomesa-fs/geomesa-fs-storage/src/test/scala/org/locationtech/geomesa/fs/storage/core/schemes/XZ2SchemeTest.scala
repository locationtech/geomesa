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
  val sftPoly = SimpleFeatureTypes.createType("test", "*geom:Polygon:srid=4326")

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

    "calculate covering filters for points" in {
      val r = new Random(77)
      val features = Seq.tabulate(100) { i =>
        ScalaSimpleFeature.create(sft, s"$i", s"POINT (${r.nextDouble(-180, 180)} ${r.nextDouble(-90, 90)})")
      }

      foreach(Seq(4, 8, 12, 16)) { bits =>
        val ps = PartitionSchemeFactory.load(sft, s"xz2:bits=$bits")
        ps must beAnInstanceOf[XZ2Scheme]
        foreach(features.groupBy(ps.getPartition)) { case (partition, group) =>
          val filter = ps.getCoveringFilter(partition)
          features.filter(filter.evaluate) must containTheSameElementsAs(group)
        }
      }
    }

    "calculate covering filters for polygons" in {
      val r = new Random(42)

      // generate polygons of various sizes
      val features = Seq.tabulate(100) { i =>
        val centerX = r.nextDouble(-180, 180)
        val centerY = r.nextDouble(-90, 90)

        // vary the size from small (0.1 degrees) to large (45 degrees)
        val size = r.nextDouble(0.1, 45.0) / 2

        val xmin = math.max(-180, centerX - size)
        val ymin = math.max(-90, centerY - size)
        val xmax = math.min(180, centerX + size)
        val ymax = math.min(90, centerY + size)

        ScalaSimpleFeature.create(sftPoly, s"$i", s"POLYGON (($xmin $ymin, $xmax $ymin, $xmax $ymax, $xmin $ymax, $xmin $ymin))")
      }

      foreach(Seq(4, 8, 12, 16)) { bits =>
        val ps = PartitionSchemeFactory.load(sftPoly, s"xz2:bits=$bits")
        ps must beAnInstanceOf[XZ2Scheme]
        foreach(features.groupBy(ps.getPartition)) { case (partition, group) =>
          val filter = ps.getCoveringFilter(partition)
          features.filter(filter.evaluate) must containTheSameElementsAs(group)
        }
      }
    }

    "calculate covering filters for mixed geometry sizes" in {
      val r = new Random(123)

      // mix of very small and very large polygons
      val features = Seq.tabulate(50) { i =>
        val centerX = r.nextDouble(-180, 180)
        val centerY = r.nextDouble(-90, 90)

        // alternate between tiny and huge polygons
        val size = (if (i % 2 == 0) { r.nextDouble(0.01, 0.1) } else { r.nextDouble(30.0, 90.0) }) / 2

        val xmin = math.max(-180, centerX - size)
        val ymin = math.max(-90, centerY - size)
        val xmax = math.min(180, centerX + size)
        val ymax = math.min(90, centerY + size)

        ScalaSimpleFeature.create(sftPoly, s"$i", s"POLYGON (($xmin $ymin, $xmax $ymin, $xmax $ymax, $xmin $ymax, $xmin $ymin))")
      }

      foreach(Seq(4, 8, 12, 16)) { bits =>
        val ps = PartitionSchemeFactory.load(sftPoly, s"xz2:bits=$bits")
        ps must beAnInstanceOf[XZ2Scheme]
        foreach(features.groupBy(ps.getPartition)) { case (partition, group) =>
          val filter = ps.getCoveringFilter(partition)
          val matched = features.filter(filter.evaluate)
          matched must containTheSameElementsAs(group)
        }
      }
    }
  }
}
