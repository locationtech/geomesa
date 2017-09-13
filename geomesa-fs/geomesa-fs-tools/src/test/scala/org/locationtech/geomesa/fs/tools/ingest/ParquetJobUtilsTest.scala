/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.time.temporal.ChronoUnit

import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.common.{DateTimeScheme, PartitionScheme}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParquetJobUtilsTest extends Specification {



  "ParquetJobUtils" should {
    "properly serialize sft with partition scheme user data" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val partitionScheme = new DateTimeScheme(DateTimeScheme.Formats.Daily, ChronoUnit.DAYS, 1, "dtg", false)
      val conf = new Configuration
      PartitionScheme.addToSft(sft, partitionScheme)
      ParquetJobUtils.setSimpleFeatureType(conf, sft)

      val newSFT = ParquetJobUtils.getSimpleFeatureType(conf)
      val extractedScheme = PartitionScheme.extractFromSft(newSFT)
      extractedScheme.name mustEqual partitionScheme.name()
    }
  }


}