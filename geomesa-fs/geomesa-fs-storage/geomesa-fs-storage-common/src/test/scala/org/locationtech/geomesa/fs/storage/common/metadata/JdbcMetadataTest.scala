/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6c49bcd685 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 630900cfbb (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> a928f2f73 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 6c49bcd685 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 630900cfbb (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
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
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
=======
>>>>>>> a928f2f73 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 6c49bcd685 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
=======
>>>>>>> a928f2f73 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 630900cfbb (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
=======
>>>>>>> a928f2f73 (GEOMESA-3246 Upgrade Arrow to 11.0.0)

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import java.io.{File, FileOutputStream}
import java.nio.file.Files

@RunWith(classOf[JUnitRunner])
class JdbcMetadataTest extends Specification with LazyLogging with BeforeAfterAll {

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

  var container: PostgreSQLContainer[_] = _

  // noinspection LanguageFeature
  implicit def toBounds(env: Envelope): Option[PartitionBounds] = PartitionBounds(env)

  override def beforeAll(): Unit = {
    val image = DockerImageName.parse("postgres").withTag(sys.props.getOrElse("postgres.docker.tag", "15.1"))
    container = new PostgreSQLContainer(image)
    // if we don't set the default db/name to postgres, the startup check fails as it restarts 3 times instead of the expected 2
    container.withDatabaseName("postgres")
    container.withUsername("postgres")
    container.start()
    container.followOutput(new Slf4jLogConsumer(logger.underlying))
  }

  override def afterAll(): Unit = {
    if (container != null) {
      container.stop()
    }
  }

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
    "set and get key-value pairs" in {
      withPath { context =>
        val config = getConfig(context.root)
        WithClose(factory.create(context, config, meta)) { created =>
          created.set("foo", "bar")
          created.set("bar", "baz")
          val loaded = factory.load(context)
          loaded must beSome
          try {
            foreach(Seq(created, loaded.get)) { metadata =>
              metadata.encoding mustEqual encoding
              metadata.sft mustEqual sft
              metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
              metadata.scheme mustEqual scheme
              metadata.get("foo") must beSome("bar")
              metadata.get("bar") must beSome("baz")
            }
          } finally {
            loaded.get.close()
          }
        }
      }
    }
    "read old tables" in {
      WithClose(getClass.getClassLoader.getResourceAsStream("jdbc/old_meta.sql")) { db =>
        db must not(beNull)

        withPath { context =>
          val config = getConfig(context.root)
          WithClose(JdbcMetadataFactory.createDataSource(config)) { source =>
            WithClose(source.getConnection()) { connection =>
              WithClose(connection.createStatement()) { statement =>
                // splitting on ; may not be universally safe, but works for our script
                IOUtils.toString(db, StandardCharsets.UTF_8).split(";").foreach { sql =>
                  statement.execute(s"$sql;")
                }
              }
              // update the root col in the metadata table to point to our current root
              Seq("storage_meta", "storage_partitions", "storage_partition_files").foreach { table =>
                WithClose(connection.prepareStatement(s"update $table set root = ?")) { ps =>
                  ps.setString(1, context.root.toUri.toString)
                  ps.executeUpdate()
                }
              }
            }
          }

          // create the metadata.json file pointing to the table
          MetadataJson.writeMetadata(context, NamedOptions(factory.name, config))

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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
=======
>>>>>>> a928f2f73 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 6c49bcd685 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 630900cfbb (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
  def getConfig(root: Path): Map[String, String] = {
    // the tmp dir is all numbers - change it to chars to make a valid, unique db name for each test
    val db = new String(root.getName.replace("geomesa", "").toCharArray.map(c => 'a' + c.toInt).map(_.toChar))
    WithClose(container.createConnection("")) { connection =>
      WithClose(connection.createStatement()) { statement =>
        statement.execute(s"create database $db")
      }
    }
    Map(
      JdbcMetadata.Config.UrlKey      -> container.getJdbcUrl.replace(s"/${container.getDatabaseName}", s"/$db"),
      JdbcMetadata.Config.UserKey     -> container.getUsername,
      JdbcMetadata.Config.PasswordKey -> container.getPassword,
    )
  }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6c49bcd685 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 630900cfbb (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
  def getConfig(root: Path): Map[String, String] =
    Map(JdbcMetadata.Config.UrlKey -> s"jdbc:h2:split:${new File(root.toString).getAbsolutePath}/metadata;CASE_INSENSITIVE_IDENTIFIERS=true")
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
  def getConfig(root: Path): Map[String, String] =
    Map(JdbcMetadata.Config.UrlKey -> s"jdbc:h2:split:${new File(root.toString).getAbsolutePath}/metadata;CASE_INSENSITIVE_IDENTIFIERS=true")
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> a928f2f73 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 6c49bcd685 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
=======
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
  def getConfig(root: Path): Map[String, String] =
    Map(JdbcMetadata.Config.UrlKey -> s"jdbc:h2:split:${new File(root.toString).getAbsolutePath}/metadata;CASE_INSENSITIVE_IDENTIFIERS=true")
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> a928f2f73 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 630900cfbb (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
=======
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
  def getConfig(root: Path): Map[String, String] =
    Map(JdbcMetadata.Config.UrlKey -> s"jdbc:h2:split:${new File(root.toString).getAbsolutePath}/metadata;CASE_INSENSITIVE_IDENTIFIERS=true")
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> a928f2f73 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
}
