/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.api.data._
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.index.metadata.TableBasedMetadata
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
class AccumuloBackedMetadataTest extends Specification with TestWithFeatureType {

  import scala.collection.JavaConverters._

  override val spec = "name:String,geom:Point:srid=4326,dtg:Date"

  "AccumuloDataStore" should {
    "re-check for catalog table existing" in {
      TableBasedMetadata.Expiry.threadLocalValue.set("100ms")
      try {
        WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { newDs =>
          newDs.getTypeNames must beEmpty // ensure table existence is checked/cached
          val typeName = sft.getTypeName // note: triggers lazy evaluation which creates the schema
          eventually(10, 100.millis)(newDs.getTypeNames mustEqual Array(typeName))
        }
      } finally {
        TableBasedMetadata.Expiry.threadLocalValue.remove()
      }
    }
  }
}
