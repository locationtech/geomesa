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
import java.util.Collections

import org.locationtech.jts.geom.Envelope
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileContext, Path}
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api.PartitionMetadata
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class FileMetadataTest extends Specification with AllExpectations {

  import scala.collection.JavaConverters._

  lazy val fc = FileContext.getFileContext(new Configuration())
  val sft = SimpleFeatureTypes.createType("metadata", "name:String,dtg:Date,*geom:Point:srid=4326")
  val encoding = "parquet"
  val scheme = PartitionScheme(sft, "hourly,z2-2bit", Collections.emptyMap())

  "FileMetadata" should {
    "create and persist an empty metadata file" in {
      withPath { path =>
        val created = StorageMetadata.create(fc, path, sft, encoding, scheme)
        val loaded = StorageMetadata.load(fc, path).get
        foreach(Seq(created, loaded)) { metadata =>
          metadata.getEncoding mustEqual encoding
          metadata.getSchema mustEqual sft
          metadata.getPartitionScheme mustEqual scheme
          metadata.getPartitions.asScala must beEmpty
        }
      }
    }
    "persist file changes" in {
      withPath { path =>
        val created = StorageMetadata.create(fc, path, sft, encoding, scheme)
        created.addPartition(new PartitionMetadata("1", Collections.singletonList("file1"), 10L, new Envelope(-10, 10, -5, 5)))
        created.addPartition(new PartitionMetadata("1", java.util.Arrays.asList("file2", "file3"), 20L, new Envelope(-11, 11, -5, 5)))
        created.addPartition(new PartitionMetadata("2", java.util.Arrays.asList("file5", "file6"), 20L, new Envelope(-1, 1, -5, 5)))
        val loaded = StorageMetadata.load(fc, path).get
        foreach(Seq(created, loaded)) { metadata =>
          metadata.getEncoding mustEqual encoding
          metadata.getSchema mustEqual sft
          metadata.getPartitionScheme mustEqual scheme
          metadata.getPartitions.asScala.map(_.name) must containTheSameElementsAs(Seq("1", "2"))
          metadata.getPartition("1").files.asScala must containTheSameElementsAs((1 to 3).map(i => s"file$i"))
          metadata.getPartition("2").files.asScala must containTheSameElementsAs((5 to 6).map(i => s"file$i"))
        }
      }
    }
    "read metadata from nested folders" in {
      withPath { path =>
        val created = StorageMetadata.create(fc, path, sft, encoding, scheme)
        created.addPartition(new PartitionMetadata("1", Collections.singletonList("file1"), 10L, new Envelope(-10, 10, -5, 5)))
        created.addPartition(new PartitionMetadata("1", java.util.Arrays.asList("file2", "file3"), 20L, new Envelope(-11, 11, -5, 5)))
        fc.mkdir(new Path(path, "metadata/nested/"), FsPermission.getDirDefault, false)
        fc.util.listStatus(new Path(path, "metadata")).foreach { file =>
          if (file.getPath.getName.startsWith("update-")) {
            fc.rename(file.getPath, new Path(path, s"metadata/nested/${file.getPath.getName}"))
          }
        }
        created.reload()
        val loaded = StorageMetadata.load(fc, path).get
        foreach(Seq(created, loaded)) { metadata =>
          metadata.getEncoding mustEqual encoding
          metadata.getSchema mustEqual sft
          metadata.getPartitionScheme mustEqual scheme
          metadata.getPartitions.asScala.map(_.name) mustEqual Seq("1")
          metadata.getPartition("1").files.asScala must containTheSameElementsAs((1 to 3).map(i => s"file$i"))
        }

      }
    }
    "transition old metadata files" in {
      withPath { path =>
        val metadata = new Path(path, "metadata.json")
        fc.util.copy(new Path(getClass.getClassLoader.getResource("metadata-old.json").toURI), metadata)
        fc.util.exists(metadata) must beTrue
        val option = StorageMetadata.load(fc, path)
        option must beSome
        val storage = option.get
        storage.getEncoding mustEqual "orc"
        storage.getSchema.getTypeName mustEqual "example-csv"
        storage.getPartitionScheme.getName mustEqual "datetime"
        storage.getPartitions.asScala must containTheSameElementsAs(
          Seq(
            new PartitionMetadata("2015/05/06", Collections.singletonList("06_Wb48cb7293793447480c0885f3f4bb56a.orc"), 0L, new Envelope),
            new PartitionMetadata("2015/06/07", Collections.singletonList("07_W25d311113f0b4bad819f209f00a58173.orc"), 0L, new Envelope),
            new PartitionMetadata("2015/10/23", Collections.singletonList("23_Weedeb59bad0d4521b2ae46189eac4a4d.orc"), 0L, new Envelope)
          )
        )
        fc.util.exists(metadata) must beFalse // ensure old file was deleted
      }
    }
  }

  def withPath[R](code: Path => R): R = {
    val file = Files.createTempDirectory("geomesa").toFile.getPath
    try { code(new Path(file)) } finally {
      FileUtils.deleteDirectory(new File(file))
    }
  }
}
