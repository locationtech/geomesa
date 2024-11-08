/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.api.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFile
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, Metadata, NamedOptions}
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataFactory
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorageFactory
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import java.nio.file.Files


@RunWith(classOf[JUnitRunner])
class CompactionTest extends Specification with AllExpectations {

  sequential

  lazy val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  lazy val tempDir = Files.createTempDirectory("geomesa")
  lazy val fs = FileSystem.get(tempDir.toUri, new Configuration())

  "ParquetFileSystemStorage" should {
    "compact partitions" >> {
      val conf = new Configuration()
      conf.set("parquet.compression", "gzip")
      val context = FileSystemContext(fs, conf, new Path(tempDir.toUri))

      val metadata =
        new FileBasedMetadataFactory()
            .create(context, Map.empty, Metadata(sft, "parquet", NamedOptions("daily"), leafStorage = true))
      val fsStorage = new ParquetFileSystemStorageFactory().apply(context, metadata)

      val dtg = "2017-01-01"

      val sf1 = ScalaSimpleFeature.create(sft, "1", "first", 100, dtg, "POINT (10 10)")
      val partition = fsStorage.metadata.scheme.getPartitionName(sf1)

      partition mustEqual "2017/01/01"

      def write(sf: ScalaSimpleFeature): Unit = {
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        val writer = fsStorage.getWriter(partition)
        val id = sf.getID
        writer.write(sf)
        var i = 1
        while (i < 100) {
          sf.setId(id + f"$i%02d")
          writer.write(sf)
          i += 1
        }
        writer.close()
      }

      // First simple feature goes in its own file
      write(sf1)
      fsStorage.metadata.getPartition(partition).map(_.files) must beSome(haveSize[Seq[StorageFile]](1))
      SelfClosingIterator(fsStorage.getReader(Query.ALL, Some(partition))).toList must haveSize(100)

      // Second simple feature should be in a separate file
      val sf2 = ScalaSimpleFeature.create(sft, "2", "second", 200, dtg, "POINT (10 10)")
      write(sf2)
      fsStorage.metadata.getPartition(partition).map(_.files) must beSome(haveSize[Seq[StorageFile]](2))
      SelfClosingIterator(fsStorage.getReader(Query.ALL, Some(partition))).toList must haveSize(200)

      // Third feature in a third file
      val sf3 = ScalaSimpleFeature.create(sft, "3", "third", 300, dtg, "POINT (10 10)")
      write(sf3)
      fsStorage.metadata.getPartition(partition).map(_.files) must beSome(haveSize[Seq[StorageFile]](3))
      SelfClosingIterator(fsStorage.getReader(Query.ALL, Some(partition))).toList must haveSize(300)

      // Compact to create a single file
      fsStorage.compact(Some(partition))
      fsStorage.metadata.getPartition(partition).map(_.files) must beSome(haveSize[Seq[StorageFile]](1))
      SelfClosingIterator(fsStorage.getReader(Query.ALL, Some(partition))).toList must haveSize(300)

      // delete a feature and compact again
      WithClose(fsStorage.getWriter(ECQL.toFilter("IN ('2')"))) { writer =>
        writer.hasNext must beTrue
        writer.next
        writer.remove()
      }
      fsStorage.metadata.getPartition(partition).map(_.files) must beSome(haveSize[Seq[StorageFile]](2))
      fsStorage.compact(Some(partition))
      fsStorage.metadata.getPartition(partition).map(_.files) must beSome(haveSize[Seq[StorageFile]](1))
      SelfClosingIterator(fsStorage.getReader(Query.ALL, Some(partition))).toList must haveSize(299)

      // compact to a given file size
      // verify if file is appropriately sized, it won't be modified
      val paths = fsStorage.getFilePaths(partition).map(_.path)
      val size = paths.map(f => fs.getFileStatus(f).getLen).sum
      fsStorage.compact(Some(partition), Some(size))
      fsStorage.getFilePaths(partition).map(_.path) mustEqual paths
      // verify files are split into smaller ones
      fsStorage.compact(Some(partition), Some(size / 2))
      fsStorage.metadata.getPartition(partition).map(_.files.length).getOrElse(0) must beGreaterThan(1)
      SelfClosingIterator(fsStorage.getReader(Query.ALL, Some(partition))).toList must haveSize(299)
    }
  }

  step {
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
