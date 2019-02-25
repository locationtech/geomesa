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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata}
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, Metadata, NamedOptions, PartitionSchemeFactory}
import org.locationtech.geomesa.fs.storage.common.metadata.{JdbcMetadata, JdbcMetadataFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Envelope
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class JdbcMetadataTest extends Specification with AllExpectations {

  sequential

  import scala.collection.JavaConverters._

  lazy val conf = new Configuration()
  lazy val fc = FileContext.getFileContext(conf)
  val sft = SimpleFeatureTypes.createType("metadata",
    "name:String,dtg:Date,*geom:Point:srid=4326;geomesa.user-data.prefix=desc,desc.name=姓名,desc.dtg=ひづけ,desc.geom=좌표")
  val encoding = "parquet"
  val schemeOptions = NamedOptions("hourly,z2-2bits", Map.empty)
  val scheme = PartitionSchemeFactory.load(sft, schemeOptions)
  val meta = Metadata(sft, encoding, schemeOptions, leafStorage = true)
  val factory = new JdbcMetadataFactory()

  // noinspection LanguageFeature
  implicit def toBounds(env: Envelope): Option[PartitionBounds] = PartitionBounds(env)

  "JdbcMetadata" should {
    "not load an non-existing table" in {
      withPath { context =>
        factory.load(context) must beNone
      }
    }
    "create and persist an empty metadata" in {
      withPath { context =>
        val config = getConfig(context.root)
        WithClose(factory.create(context, config, meta)) { created =>
          val loaded = factory.load(context)
          loaded must beSome
          try {
            foreach(Seq(created, loaded.get)) { metadata =>
              metadata.encoding mustEqual encoding
              metadata.sft mustEqual sft
              metadata.scheme mustEqual scheme
              metadata.getPartitions() must beEmpty
            }
          } finally {
            loaded.get.close()
          }
        }
      }
    }
    "persist file changes" in {
      withPath { context =>
        val config = getConfig(context.root)
        WithClose(factory.create(context, config, meta)) { created =>
          created.addPartition(PartitionMetadata("1", Seq("file1"), new Envelope(-10, 10, -5, 5), 10L))
          created.addPartition(PartitionMetadata("1", Seq("file2", "file3"), new Envelope(-11, 11, -5, 5), 20L))
          created.addPartition(PartitionMetadata("2", Seq("file5", "file6"), new Envelope(-1, 1, -5, 5), 20L))
          val loaded = factory.load(context)
          loaded must beSome
          try {
            foreach(Seq(created, loaded.get)) { metadata =>
              metadata.encoding mustEqual encoding
              metadata.sft mustEqual sft
              metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
              metadata.scheme mustEqual scheme
              metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
              metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs((1 to 3).map(i => s"file$i")))
              metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs((5 to 6).map(i => s"file$i")))
            }
          } finally {
            loaded.get.close()
          }
        }
      }
    }
    "delete and compact" in {
      withPath { context =>
        val config = getConfig(context.root)
        WithClose(factory.create(context, config, meta)) { created =>
          created.addPartition(PartitionMetadata("1", Seq("file1"), new Envelope(-10, 10, -5, 5), 10L))
          created.addPartition(PartitionMetadata("1", Seq("file2", "file3"), new Envelope(-11, 11, -5, 5), 20L))
          created.addPartition(PartitionMetadata("2", Seq("file5", "file6"), new Envelope(-1, 1, -5, 5), 20L))
          created.removePartition(PartitionMetadata("1", Seq("file2"), new Envelope(-11, 11, -5, 5), 5L))
          val loaded = factory.load(context)
          loaded must beSome
          try {
            foreach(Seq(created, loaded.get)) { metadata =>
              metadata.encoding mustEqual encoding
              metadata.sft mustEqual sft
              metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
              metadata.scheme mustEqual scheme
              metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
              metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq("file1", "file3")))
              metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq("file5", "file6")))
            }
          } finally {
            loaded.get.close()
          }
          created.compact(None)
          created.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
          created.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq("file1", "file3")))
          created.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq("file5", "file6")))
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

  def getConfig(root: Path): Map[String, String] =
    Map(JdbcMetadata.Config.UrlKey -> s"jdbc:h2:split:${new File(root.toString).getAbsolutePath}/metadata")
}
