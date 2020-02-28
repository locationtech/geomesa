/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.io.{File, FileOutputStream}
import java.nio.file.Files

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, Metadata, NamedOptions, PartitionSchemeFactory}
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

  val f1 = StorageFile("file1", 0L)
  val Seq(f2, f3) = Seq("file2", "file3").map(StorageFile(_, 1L))
  val Seq(f5, f6) = Seq("file5", "file6").map(StorageFile(_, 2L))

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
        val f1 = StorageFile("file1", 0L)
        val Seq(f2, f3) = Seq("file2", "file3").map(StorageFile(_, 1L))
        val Seq(f5, f6) = Seq("file5", "file6").map(StorageFile(_, 2L))
        val config = getConfig(context.root)
        WithClose(factory.create(context, config, meta)) { created =>
          created.addPartition(PartitionMetadata("1", Seq(f1), new Envelope(-10, 10, -5, 5), 10L))
          created.addPartition(PartitionMetadata("1", Seq(f2, f3), new Envelope(-11, 11, -5, 5), 20L))
          created.addPartition(PartitionMetadata("2", Seq(f5, f6), new Envelope(-1, 1, -5, 5), 20L))
          val loaded = factory.load(context)
          loaded must beSome
          try {
            foreach(Seq(created, loaded.get)) { metadata =>
              metadata.encoding mustEqual encoding
              metadata.sft mustEqual sft
              metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
              metadata.scheme mustEqual scheme
              metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
              metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f2, f3)))
              metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6)))
            }
          } finally {
            loaded.get.close()
          }
        }
      }
    }
    "track modified and deleted files" in {
      withPath { context =>
        val f1 = StorageFile("file1", 0L)
        val Seq(f2, f3) = Seq("file2", "file3").map(StorageFile(_, 1L))
        val Seq(f5, f6) = Seq("file5", "file6").map(StorageFile(_, 2L))
        val f5mod = StorageFile("file5", 3L, StorageFileAction.Delete)
        val f2mod = StorageFile("file2", 3L, StorageFileAction.Modify)
        val config = getConfig(context.root)
        WithClose(factory.create(context, config, meta)) { created =>
          created.addPartition(PartitionMetadata("1", Seq(f1), new Envelope(-10, 10, -5, 5), 10L))
          created.addPartition(PartitionMetadata("1", Seq(f2, f3), new Envelope(-11, 11, -5, 5), 20L))
          created.addPartition(PartitionMetadata("2", Seq(f5, f6), new Envelope(-1, 1, -5, 5), 20L))
          created.addPartition(PartitionMetadata("2", Seq(f5mod), new Envelope(-1, 1, -5, 5), 20L))
          created.addPartition(PartitionMetadata("1", Seq(f2mod), new Envelope(-11, 11, -5, 5), 20L))
          val loaded = factory.load(context)
          loaded must beSome
          try {
            foreach(Seq(created, loaded.get)) { metadata =>
              metadata.encoding mustEqual encoding
              metadata.sft mustEqual sft
              metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
              metadata.scheme mustEqual scheme
              metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
              metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f2, f3, f2mod)))
              metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6,  f5mod)))
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
          created.addPartition(PartitionMetadata("1", Seq(f1), new Envelope(-10, 10, -5, 5), 10L))
          created.addPartition(PartitionMetadata("1", Seq(f2, f3), new Envelope(-11, 11, -5, 5), 20L))
          created.addPartition(PartitionMetadata("2", Seq(f5, f6), new Envelope(-1, 1, -5, 5), 20L))
          created.removePartition(PartitionMetadata("1", Seq(f2), new Envelope(-11, 11, -5, 5), 5L))
          val loaded = factory.load(context)
          loaded must beSome
          try {
            foreach(Seq(created, loaded.get)) { metadata =>
              metadata.encoding mustEqual encoding
              metadata.sft mustEqual sft
              metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
              metadata.scheme mustEqual scheme
              metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
              metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f3)))
              metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6)))
            }
          } finally {
            loaded.get.close()
          }
          created.compact(None)
          created.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
          created.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f3)))
          created.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6)))
        }
      }
    }
    "read old tables" in {
      WithClose(getClass.getClassLoader.getResourceAsStream("jdbc/metadata.h2")) { db =>
        db must not(beNull)

        withPath { context =>
          // copy the db file (with the old table format) into our root
          WithClose(new FileOutputStream(new File(context.root.toString, "metadata.mv.db")))(IOUtils.copy(db, _))
          // write out a metadata file pointing to the db data file
          val config = getConfig(context.root)
          MetadataJson.writeMetadata(context, NamedOptions(factory.name, config))
          // update the root col in the metadata table to point to our current root
          WithClose(JdbcMetadataFactory.createDataSource(config)) { source =>
            Seq("storage_meta", "storage_partitions", "storage_partition_files").foreach { table =>
              WithClose(source.getConnection()) { connection =>
                WithClose(connection.prepareStatement(s"update $table set root = ?")) { ps =>
                  ps.setString(1, context.root.toUri.toString)
                  ps.executeUpdate()
                }
              }
            }
          }

          val loaded = factory.load(context)
          loaded must beSome
          WithClose(loaded.get) { metadata =>
            metadata.encoding mustEqual encoding
            metadata.sft mustEqual sft
            metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
            metadata.scheme mustEqual scheme
            metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
            metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f3).map(_.copy(timestamp = 0L))))
            metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6).map(_.copy(timestamp = 0L))))
          }
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
