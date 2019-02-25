/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api.NamedOptions
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParquetJobUtilsTest extends Specification {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  "ParquetJobUtils" should {
    "properly serialize sft with partition scheme user data" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val scheme = NamedOptions("daily")
      sft.setScheme(scheme.name, scheme.options)
      val conf = new Configuration
      StorageConfiguration.setSft(conf, sft)

      val newSFT = StorageConfiguration.getSft(conf)
      val extractedScheme = newSFT.removeScheme()
      extractedScheme must beSome
      extractedScheme.get mustEqual scheme
    }
  }
}
