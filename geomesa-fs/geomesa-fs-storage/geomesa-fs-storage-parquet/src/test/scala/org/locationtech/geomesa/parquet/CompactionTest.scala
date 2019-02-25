/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.geotools.data.Query
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, Metadata, NamedOptions}
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations


@RunWith(classOf[JUnitRunner])
class CompactionTest extends Specification with AllExpectations {

  sequential

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val tempDir = Files.createTempDirectory("geomesa")
  val fc = FileContext.getFileContext(tempDir.toUri)

  "ParquetFileSystemStorage" should {
    "compact partitions" >> {
      val conf = new Configuration()
      conf.set("parquet.compression", "gzip")
      val context = FileSystemContext(fc, conf, new Path(tempDir.toUri))

      val metadata =
        new FileBasedMetadataFactory()
            .create(context, Map.empty, Metadata(sft, "parquet", NamedOptions("daily"), leafStorage = true))
      val fsStorage = new ParquetFileSystemStorageFactory().apply(context, metadata)

      val dtg = "2017-01-01"

      val sf1 = ScalaSimpleFeature.create(sft, "1", "first", 100, dtg, "POINT (10 10)")
      val partition = fsStorage.metadata.scheme.getPartitionName(sf1)

      partition mustEqual "2017/01/01"

      def write(sf: SimpleFeature): Unit = {
        val writer = fsStorage.getWriter(partition)
        writer.write(sf)
        writer.close()
      }

      // First simple feature goes in its own file
      write(sf1)
      fsStorage.metadata.getPartition(partition).map(_.files) must beSome(haveSize[Seq[String]](1))
      SelfClosingIterator(fsStorage.getReader(Query.ALL, Some(partition))).toList must haveSize(1)

      // Second simple feature should be in a separate file
      val sf2 = ScalaSimpleFeature.create(sft, "2", "second", 200, dtg, "POINT (10 10)")
      write(sf2)
      fsStorage.metadata.getPartition(partition).map(_.files) must beSome(haveSize[Seq[String]](2))
      SelfClosingIterator(fsStorage.getReader(Query.ALL, Some(partition))).toList must haveSize(2)

      // Third feature in a third file
      val sf3 = ScalaSimpleFeature.create(sft, "3", "third", 300, dtg, "POINT (10 10)")
      write(sf3)
      fsStorage.metadata.getPartition(partition).map(_.files) must beSome(haveSize[Seq[String]](3))
      SelfClosingIterator(fsStorage.getReader(Query.ALL, Some(partition))).toList must haveSize(3)

      // Compact to create a single file
      fsStorage.compact(Some(partition))
      fsStorage.metadata.getPartition(partition).map(_.files) must beSome(haveSize[Seq[String]](1))
      SelfClosingIterator(fsStorage.getReader(Query.ALL, Some(partition))).toList must haveSize(3)
    }
  }

  step {
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
