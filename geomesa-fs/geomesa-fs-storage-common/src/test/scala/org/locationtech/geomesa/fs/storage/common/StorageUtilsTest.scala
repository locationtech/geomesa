/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.io.File
import java.time.temporal.ChronoUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class StorageUtilsTest extends Specification with AllExpectations {

  "StorageUtils" should {
    "build a partition list" >> {
      val files = List(
        "/tmp/flkjsdfg/mytype/2016/02/03/00.parquet",
        "/tmp/flkjsdfg/mytype/2016/02/03/01.parquet",
        "/tmp/flkjsdfg/mytype/2016/02/03/02.parquet",
        "/tmp/flkjsdfg/mytype/2016/02/04/03.parquet",
        "/tmp/flkjsdfg/mytype/2016/02/04/04.parquet",
        "/tmp/flkjsdfg/mytype/2016/02/05/05.parquet",
        "/tmp/flkjsdfg/mytype/2016/02/05/06.parquet"
      )
      files.foreach { f => new File(f).getParentFile.mkdirs() }
      files.foreach { f => new File(f).createNewFile() }
      val root = new Path("/tmp/flkjsdfg")
      val fs = root.getFileSystem(new Configuration)
      val typeName = "mytype"
      val sft = SimpleFeatureTypes.createType(typeName, "age:Int,date:Date,*geom:Point:srid=4326")
      val scheme = new DateTimeScheme(DateTimeScheme.Formats.Hourly, ChronoUnit.HOURS, 1, sft, "date", true)
      val list = StorageUtils.buildPartitionList(new Path("/tmp/flkjsdfg/"), fs, typeName, scheme, "parquet")
      list.size mustEqual 7
      val expected = List(
        "2016/02/03/00",
        "2016/02/03/01",
        "2016/02/03/02",
        "2016/02/04/03",
        "2016/02/04/04",
        "2016/02/05/05",
        "2016/02/05/06"
      )
      list must containTheSameElementsAs(expected)
    }
  }
}
