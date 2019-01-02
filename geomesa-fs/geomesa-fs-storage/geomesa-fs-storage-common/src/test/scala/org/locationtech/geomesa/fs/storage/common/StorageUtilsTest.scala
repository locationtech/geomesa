/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.io.File
import java.nio.file.Files
import java.time.temporal.ChronoUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.common.partitions.DateTimeScheme
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.PathUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class StorageUtilsTest extends Specification with AllExpectations {

  sequential

  var tempDir: java.nio.file.Path = _

  step {
    tempDir = Files.createTempDirectory("geomesa")
  }

  "StorageUtils" should {
    "build a partition and file list" >> {
      import org.locationtech.geomesa.fs.storage.common.partitions.DateTimeScheme.Formats.Hourly

      val files = List(
        s"$tempDir/mytype/2016/02/03/00_08.parquet",
        s"$tempDir/mytype/2016/02/03/01_02.parquet",
        s"$tempDir/mytype/2016/02/03/02_03.parquet",
        s"$tempDir/mytype/2016/02/04/03_04.parquet",
        s"$tempDir/mytype/2016/02/04/04_04.parquet",
        s"$tempDir/mytype/2016/02/05/05_99.parquet",
        s"$tempDir/mytype/2016/02/05/06_00.parquet"
      )
      files.foreach { f => new File(f).getParentFile.mkdirs() }
      files.foreach { f => new File(f).createNewFile() }
      val root = new Path(tempDir.toFile.getPath)
      val fc = FileContext.getFileContext(root.toUri, new Configuration)
      val typeName = "mytype"
      val sft = SimpleFeatureTypes.createType(typeName, "age:Int,date:Date,*geom:Point:srid=4326")
      val scheme = new DateTimeScheme(Hourly.format, ChronoUnit.HOURS, 1, "date", true)
      import scala.collection.JavaConversions._
      val partitionsAndFiles = StorageUtils.partitionsAndFiles(fc, new Path(root, typeName), scheme, "parquet")
      val list = partitionsAndFiles.keySet().toList
      list must haveLength(7)
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

      val expectedMap = Map (
        "2016/02/03/00" -> Seq("00_08.parquet"),
        "2016/02/03/01" -> Seq("01_02.parquet"),
        "2016/02/03/02" -> Seq("02_03.parquet"),
        "2016/02/04/03" -> Seq("03_04.parquet"),
        "2016/02/04/04" -> Seq("04_04.parquet"),
        "2016/02/05/05" -> Seq("05_99.parquet"),
        "2016/02/05/06" -> Seq("06_00.parquet")
      )

      import scala.collection.JavaConversions._

      foreach(partitionsAndFiles.keySet) { k =>
        partitionsAndFiles(k).toSeq must containTheSameElementsAs(expectedMap(k))
      }
    }
  }

  step {
    PathUtils.deleteRecursively(tempDir)
  }
}
