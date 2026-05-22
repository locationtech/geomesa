/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.Z2Encoder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Point
import org.specs2.mutable.SpecificationWithJUnit

import java.util.HexFormat

class StorageMetadataTest extends SpecificationWithJUnit {

  private val sft = SimpleFeatureTypes.createType("test", "*geom:Point:srid=4326")

  "Z2Encoder" should {
    "create truncate-able z values that align with our partition scheme" in {
      val ps = PartitionSchemeFactory.load(sft, "z2:bits=4")
      foreach(Seq(-67.5, -22.5, 22.5, 67.5)) { lat =>
        foreach(Seq(-135, -45, 45, 135)) { lon =>
          val pt = WKTUtils.read(s"POINT($lon $lat)").asInstanceOf[Point]
          val partition = ps.getPartition(ScalaSimpleFeature.create(sft, "", pt)).value.toInt
          val z2 = Z2Encoder.encode(pt)
          partition mustEqual HexFormat.fromHexDigit(z2.head)
        }
      }
    }
  }
}
