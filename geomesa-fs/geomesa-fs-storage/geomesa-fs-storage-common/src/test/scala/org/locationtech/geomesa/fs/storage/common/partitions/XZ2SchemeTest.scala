/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.PartitionSchemeFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

class XZ2SchemeTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createType("test", "*geom:Point:srid=4326")

  "XZ2Scheme" should {
    "partition based on 10 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "xz2:bits=10")
      ps must beAnInstanceOf[XZ2Scheme]
      ps.asInstanceOf[XZ2Scheme].bits mustEqual 10

      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (10 10)")).value mustEqual "1030"
      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (-75 38)")).value mustEqual "0825"
    }

    "partition based on 20 bit curve" in {
      val ps = PartitionSchemeFactory.load(sft, "xz2:bits=20")
      ps must beAnInstanceOf[XZ2Scheme]
      ps.asInstanceOf[XZ2Scheme].bits mustEqual 20

      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (10 10)")).value mustEqual "1052614"
      ps.getPartition(ScalaSimpleFeature.create(sft, "1", "POINT (-75 38)")).value mustEqual "0843360"
    }
  }
}
