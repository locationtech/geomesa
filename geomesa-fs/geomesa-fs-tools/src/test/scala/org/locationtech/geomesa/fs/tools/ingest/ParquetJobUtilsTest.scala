/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.time.temporal.ChronoUnit

import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.common.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.partitions.DateTimeScheme
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParquetJobUtilsTest extends Specification {

  "ParquetJobUtils" should {
    "properly serialize sft with partition scheme user data" >> {
      import DateTimeScheme.Formats.Daily

      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val partitionScheme = new DateTimeScheme(Daily.format, ChronoUnit.DAYS, 1, "dtg", false)
      val conf = new Configuration
      PartitionScheme.addToSft(sft, partitionScheme)
      StorageConfiguration.setSft(conf, sft)

      val newSFT = StorageConfiguration.getSft(conf)
      val extractedScheme = PartitionScheme.extractFromSft(newSFT)
      extractedScheme must beSome
      extractedScheme.get.getName mustEqual partitionScheme.getName
    }
  }
}
