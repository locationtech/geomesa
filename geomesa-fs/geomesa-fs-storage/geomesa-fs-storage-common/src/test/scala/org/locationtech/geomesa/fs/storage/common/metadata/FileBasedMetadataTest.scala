/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileContext, Path}
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, Metadata, NamedOptions, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Envelope
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class FileBasedMetadataTest extends Specification with AllExpectations {

  import scala.collection.JavaConverters._

  lazy val conf = new Configuration()
  lazy val fc = FileContext.getFileContext(conf)
  val sft = SimpleFeatureTypes.createType("metadata",
    "name:String,dtg:Date,*geom:Point:srid=4326;geomesa.user-data.prefix=desc,desc.name=姓名,desc.dtg=ひづけ,desc.geom=좌표")
  val encoding = "parquet"
  val schemeOptions = NamedOptions("hourly,z2-2bits", Map.empty)
  val scheme = PartitionSchemeFactory.load(sft, schemeOptions)
  val meta = Metadata(sft, encoding, schemeOptions, leafStorage = true)
  val factory = new FileBasedMetadataFactory()

  val f1 = StorageFile("file1", 0L)
  val Seq(f2, f3) = Seq("file2", "file3").map(StorageFile(_, 1L))
  val Seq(f5, f6) = Seq("file5", "file6").map(StorageFile(_, 2L))

  // noinspection LanguageFeature
  implicit def toBounds(env: Envelope): Option[PartitionBounds] = PartitionBounds(env)

  "FileMetadata" should {
    "create and persist an empty metadata file" in {
      withPath { context =>
        val created = factory.create(context, Map.empty, meta)
        PathCache.invalidate(fc, context.root)
        val loaded = factory.load(context)
        loaded.foreach(_.reload()) // ensure state is loaded
        loaded must beSome
        foreach(Seq(created, loaded.get)) { metadata =>
          metadata.encoding mustEqual encoding
          metadata.sft mustEqual sft
          metadata.scheme mustEqual scheme
          metadata.getPartitions() must beEmpty
        }
      }
    }
    "persist file changes" in {
      withPath { context =>
        val created = factory.create(context, Map.empty, meta)
        created.addPartition(PartitionMetadata("1", Seq(f1), new Envelope(-10, 10, -5, 5), 10L))
        created.addPartition(PartitionMetadata("1", Seq(f2,f3), new Envelope(-11, 11, -5, 5), 20L))
        created.addPartition(PartitionMetadata("2", Seq(f5, f6), new Envelope(-1, 1, -5, 5), 20L))
        PathCache.invalidate(fc, context.root)
        val loaded = factory.load(context)
        loaded.foreach(_.reload()) // ensure state is loaded
        loaded must beSome
        foreach(Seq(created, loaded.get)) { metadata =>
          metadata.encoding mustEqual encoding
          metadata.sft mustEqual sft
          metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
          metadata.scheme mustEqual scheme
          metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
          metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f2, f3)))
          metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6)))
        }
      }
    }
    "read metadata from nested folders" in {
      withPath { context =>
        val created = factory.create(context, Map.empty, meta)
        created.addPartition(PartitionMetadata("1", Seq(f1), new Envelope(-10, 10, -5, 5), 10L))
        created.addPartition(PartitionMetadata("1", Seq(f2, f3), new Envelope(-11, 11, -5, 5), 20L))
        fc.mkdir(new Path(context.root, "metadata/nested/"), FsPermission.getDirDefault, false)
        fc.util.listStatus(new Path(context.root, "metadata")).foreach { file =>
          if (file.getPath.getName.startsWith("update-")) {
            fc.rename(file.getPath, new Path(context.root, s"metadata/nested/${file.getPath.getName}"))
          }
        }
        created.reload()
        PathCache.invalidate(fc, context.root)
        val loaded = factory.load(context)
        loaded.foreach(_.reload()) // ensure state is loaded
        loaded must beSome
        foreach(Seq(created, loaded.get)) { metadata =>
          metadata.encoding mustEqual encoding
          metadata.sft mustEqual sft
          metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
          metadata.scheme mustEqual scheme
          metadata.getPartitions().map(_.name) mustEqual Seq("1")
          metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f2, f3)))
        }
      }
    }
    "track modified and deleted files" in {
      withPath { context =>
        val f5mod = StorageFile("file5", 3L, StorageFileAction.Delete)
        val f2mod = StorageFile("file2", 3L, StorageFileAction.Modify)
        val created = factory.create(context, Map.empty, meta)
        created.addPartition(PartitionMetadata("1", Seq(f1), new Envelope(-10, 10, -5, 5), 10L))
        created.addPartition(PartitionMetadata("1", Seq(f2,f3), new Envelope(-11, 11, -5, 5), 20L))
        created.addPartition(PartitionMetadata("2", Seq(f5, f6), new Envelope(-1, 1, -5, 5), 20L))
        created.addPartition(PartitionMetadata("2", Seq(f5mod), new Envelope(-1, 1, -5, 5), 20L))
        created.addPartition(PartitionMetadata("1", Seq(f2mod), new Envelope(-11, 11, -5, 5), 20L))
        PathCache.invalidate(fc, context.root)
        val loaded = factory.load(context)
        loaded.foreach(_.reload()) // ensure state is loaded
        loaded must beSome
        foreach(Seq(created, loaded.get)) { metadata =>
          metadata.encoding mustEqual encoding
          metadata.sft mustEqual sft
          metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
          metadata.scheme mustEqual scheme
          metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
          metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f2, f3, f2mod)))
          metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6, f5mod)))
        }
      }
    }
  }

  def withPath[R](code: FileSystemContext => R): R = {
    val file = Files.createTempDirectory("geomesa").toFile.getPath
    try { code(FileSystemContext(fc, conf, new Path(file))) } finally {
      FileUtils.deleteDirectory(new File(file))
    }
  }
}
