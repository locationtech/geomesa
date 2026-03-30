/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.api.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.FileSystemContext
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, PartitionKey}
import org.locationtech.geomesa.fs.storage.common.metadata.StorageMetadataCatalog
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorageFactory
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.file.Files

class CompactionTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  "ParquetFileSystemStorage" should {
    "compact partitions" in {
      val tempDir = Files.createTempDirectory("geomesa")
      try {
        val fs = FileSystem.get(tempDir.toUri, new Configuration())
        val conf = new Configuration()
        conf.set("parquet.compression", "gzip")
        val context = FileSystemContext(fs, conf, new Path(tempDir.toUri))

        val dtg = "2017-01-01"
        val sf1 = ScalaSimpleFeature.create(sft, "1", "first", 100, dtg, "POINT (10 10)")

        val catalog = StorageMetadataCatalog(context, "file", Map.empty)
        WithClose(new ParquetFileSystemStorageFactory().apply(context, catalog.create(sft, Seq("daily")))) { storage =>
          val partition = Partition(storage.metadata.schemes.map(s => PartitionKey(s.name, s.getPartition(sf1))))

          def write(sf: ScalaSimpleFeature): Unit = {
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            val writer = storage.getWriter(partition)
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

          // first simple feature goes in its own file
          write(sf1)
          storage.metadata.getFiles(partition) must haveSize(1)
          CloseableIterator(storage.getReader(Query.ALL)).toList must haveSize(100)

          // second simple feature should be in a separate file
          val sf2 = ScalaSimpleFeature.create(sft, "2", "second", 200, dtg, "POINT (10 10)")
          write(sf2)
          storage.metadata.getFiles(partition) must haveSize(2)
          CloseableIterator(storage.getReader(Query.ALL)).toList must haveSize(200)

          // third feature in a third file
          val sf3 = ScalaSimpleFeature.create(sft, "3", "third", 300, dtg, "POINT (10 10)")
          write(sf3)
          storage.metadata.getFiles(partition) must haveSize(3)
          CloseableIterator(storage.getReader(Query.ALL)).toList must haveSize(300)

          // compact to create a single file
          storage.compact(partition)
          storage.metadata.getFiles(partition) must haveSize(1)
          CloseableIterator(storage.getReader(Query.ALL)).toList must haveSize(300)

          // delete a feature and compact again
          WithClose(storage.getWriter(ECQL.toFilter("IN ('2')"))) { writer =>
            writer.hasNext must beTrue
            writer.next
            writer.remove()
          }
          storage.metadata.getFiles(partition) must haveSize(2)
          CloseableIterator(storage.getReader(Query.ALL)).toList must haveSize(299)
          storage.compact(partition)
          storage.metadata.getFiles(partition) must haveSize(1)
          CloseableIterator(storage.getReader(Query.ALL)).toList must haveSize(299)

          // compact to a given file size
          // verify if file is appropriately sized, it won't be modified
          val paths = storage.metadata.getFiles(partition).map(_.file)
          val size = paths.map(f => fs.getFileStatus(new Path(context.root, f)).getLen).sum
          storage.compact(partition, Some(size))
          storage.metadata.getFiles(partition).map(_.file) mustEqual paths
          // verify files are split into smaller ones
          storage.compact(partition, Some(size / 2))
          storage.metadata.getFiles(partition).size must beGreaterThan(1)
          CloseableIterator(storage.getReader(Query.ALL)).toList must haveSize(299)
        }
      } finally {
        FileUtils.deleteDirectory(tempDir.toFile)
      }
    }
  }
}
