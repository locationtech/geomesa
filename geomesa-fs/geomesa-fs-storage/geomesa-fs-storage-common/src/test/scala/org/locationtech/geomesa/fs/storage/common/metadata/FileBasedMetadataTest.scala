/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Envelope
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class FileBasedMetadataTest extends Specification with AllExpectations {

  import scala.collection.JavaConverters._

  lazy val conf = new Configuration()
  lazy val fs = FileSystem.get(conf)
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
        PathCache.invalidate(fs, context.root)
        factory.load(context) must beSome(created)
        foreach(Seq(created, FileBasedMetadata.copy(created))) { metadata =>
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
        PathCache.invalidate(fs, context.root)
        factory.load(context) must beSome(created)
        foreach(Seq(created, FileBasedMetadata.copy(created))) { metadata =>
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
        fs.mkdirs(new Path(context.root, "metadata/nested/"))
        fs.listStatus(new Path(context.root, "metadata")).foreach { file =>
          if (file.getPath.getName.startsWith("update-")) {
            fs.rename(file.getPath, new Path(context.root, s"metadata/nested/${file.getPath.getName}"))
          }
        }
        factory.load(context) must beSome(created)
        foreach(Seq(created, FileBasedMetadata.copy(created))) { metadata =>
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
        factory.load(context) must beSome(created)
        foreach(Seq(created, FileBasedMetadata.copy(created))) { metadata =>
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
    "unregister duplicate entries" in {
      withPath { context =>
        val mod = StorageFile("file", 2L, StorageFileAction.Append)
        val created = factory.create(context, Map.empty, meta)
        created.addPartition(PartitionMetadata("1", Seq(mod), None, 0L))
        created.addPartition(PartitionMetadata("1", Seq(mod.copy(timestamp = 3L)), None, 0L))
        created.removePartition(PartitionMetadata("1", Seq(mod.copy(timestamp = 4L)), None, 0L))
        foreach(Seq(created, FileBasedMetadata.copy(created))) { metadata =>
          metadata.getPartitions().map(_.name) mustEqual Seq("1")
          metadata.getPartition("1").map(_.files) must beSome(beEqualTo(Seq(mod)))
        }
      }
    }
    "compact files" in {
      withPath { context =>
        val f5mod = StorageFile("file5", 3L, StorageFileAction.Delete)
        val f2mod = StorageFile("file2", 3L, StorageFileAction.Modify)
        val created = factory.create(context, Map.empty, meta)
        created.addPartition(PartitionMetadata("1", Seq(f1), new Envelope(-10, 10, -5, 5), 10L))
        created.addPartition(PartitionMetadata("1", Seq(f2,f3), new Envelope(-11, 11, -5, 5), 20L))
        created.addPartition(PartitionMetadata("2", Seq(f5, f6), new Envelope(-1, 1, -5, 5), 20L))
        created.addPartition(PartitionMetadata("2", Seq(f5mod), new Envelope(-1, 1, -5, 5), 20L))
        created.addPartition(PartitionMetadata("1", Seq(f2mod), new Envelope(-11, 11, -5, 5), 20L))

        val initial = list(created.directory)
        initial must haveLength(6) // 5 updates + storage.json

        created.compact(None, threads = 2)

        val compacted = list(created.directory)
        compacted must haveLength(2)
        compacted must containTheSameElementsAs(Seq("storage.json", "compacted.conf"))

        foreach(Seq(created, FileBasedMetadata.copy(created))) { metadata =>
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
    "render compactly" in {
      withPath { context =>
        val metadata = factory.create(context, FileBasedMetadata.DefaultOptions.options, meta)
        metadata.addPartition(PartitionMetadata("1", Seq(f1), new Envelope(-10, 10, -5, 5), 10L))
        val updates = list(metadata.directory).filter(_.startsWith("update"))
        updates must haveLength(1)
        val update = WithClose(fs.open(new Path(metadata.directory, updates.head))) { in =>
          IOUtils.toString(in, StandardCharsets.UTF_8)
        }
        update must not(beEmpty)
        update must not(contain(' '))

        metadata.compact(None)
        val compactions = list(metadata.directory).filter(_.startsWith("compact"))
        compactions must haveLength(1)
        val compaction = WithClose(fs.open(new Path(metadata.directory, compactions.head))) { in =>
          IOUtils.toString(in, StandardCharsets.UTF_8)
        }
        compaction must not(beEmpty)
        compaction must not(contain(' '))
      }
    }
    "read old metadata without partition names" in {
      val url = getClass.getClassLoader.getResource("example-csv/")
      url must not(beNull)
      val path = new Path(url.toURI)
      val conf = new Configuration()
      conf.set("parquet.compression", "gzip")
      val context = FileSystemContext(path, conf)
      val metadata = StorageMetadataFactory.load(context).orNull
      metadata must beAnInstanceOf[FileBasedMetadata]
      val partitions = metadata.getPartitions()
      partitions must haveLength(3)
      partitions.flatMap(_.files.map(_.name)) must containTheSameElementsAs(
        Seq(
          "05_W341a1c90e3bd444faa96054fa6870a1c.parquet",
          "06_W4da9e1a9fe154d5ba5fd7277e5ffec60.parquet",
          "10_W3a94e68af7be42ecbef130507cdcc2fd.parquet"
        )
      )
    }
  }

  def withPath[R](code: FileSystemContext => R): R = {
    val file = Files.createTempDirectory("geomesa").toFile.getPath
    try { code(FileSystemContext(fs, conf, new Path(file))) } finally {
      FileUtils.deleteDirectory(new File(file))
    }
  }

  def list(dir: Path): Seq[String] = {
    val builder = Seq.newBuilder[String]
    val iter = fs.listStatusIterator(dir)
    while (iter.hasNext) {
      val status = iter.next
      val path = status.getPath
      builder += path.getName
      if (status.isDirectory) {
        builder ++= list(path)
      }
    }
    builder.result
  }
}
