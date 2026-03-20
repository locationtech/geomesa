/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, PartitionKey, SpatialBounds, StorageFile}
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification

import java.io.File
import java.nio.file.Files

abstract class TestAbstractMetadata extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  lazy val conf = new Configuration()
  lazy val fs = FileSystem.get(conf)
  val sft = SimpleFeatureTypes.createType("metadata",
    "name:String,dtg:Date,*geom:Point:srid=4326;geomesa.user-data.prefix=desc,desc.name=姓名,desc.dtg=ひづけ,desc.geom=좌표")
  val encoding = "parquet"
  val schemeOptions = Seq("hour", "z2:bits=2")
  val schemes = schemeOptions.map(PartitionSchemeFactory.load(sft, _)).toSet

  // note: ensure that partitions and bounds line up
  val partition1 = partitions("2026-03-05T00:00:00.000Z", "POINT (10 10)")
  val bounds1 = Seq(SpatialBounds(3, 1, 2, 11, 12), SpatialBounds(4, -1, -2, -11, -12))
  val partition2 = partitions("2026-03-04T00:00:00.000Z", "POINT (-10 10)")
  val bounds2a = Seq(SpatialBounds(3, -11, 2, -1, 12))
  val bounds2b = Seq(SpatialBounds(3, -20, 12, -12, 20))
  val partition3 = partitions("2026-03-03T00:00:00.000Z", "POINT (10 -10)")
  val bounds3 = Seq(SpatialBounds(3, 1, -12, 11, -2))

  val f1 = StorageFile("file1", partition1, 0L, spatialBounds = bounds1, timestamp = System.currentTimeMillis() - 100)
  val f2 = StorageFile("file2", partition2, 1L, spatialBounds = bounds2a, timestamp = f1.timestamp + 10)
  val f3 = StorageFile("file3", partition2, 1L, spatialBounds = bounds2b, timestamp = f2.timestamp + 10)
  val f5 = StorageFile("file5", partition3, 2L, spatialBounds = bounds3, timestamp = f3.timestamp + 20)
  val f6 = StorageFile("file6", partition3, 2L,  spatialBounds = bounds3, timestamp = f5.timestamp + 10)

  val files = Seq(f1, f2, f3, f5, f6)

  private def partitions(dtg: String, geom: String): Partition = {
    val sf = ScalaSimpleFeature.create(sft, "", "", dtg, geom)
    Partition(schemes.map(s => PartitionKey(s.name, s.getPartition(sf))).toSet)
  }

  protected def metadataType: String

  protected def getConfig(root: Path): Map[String, String]

  def newCatalog(context: FileSystemContext) = StorageMetadataCatalog(context, metadataType, getConfig(context.root))

  "Metadata" should {
    "not load an non-existing table" in {
      withPath { catalog =>
        catalog.getTypeNames must beEmpty
      }
    }
    "create and persist an empty metadata" in {
      withPath { catalog =>
        WithClose(catalog.create(sft, schemeOptions)) { created =>
          WithClose(catalog.load(sft.getTypeName)) { loaded =>
            foreach(Seq(created, loaded)) { metadata =>
              metadata.sft mustEqual sft
              metadata.schemes mustEqual schemes
              metadata.getFiles() must beEmpty
            }
          }
        }
      }
    }
    "persist file changes" in {
      withPath { catalog =>
        WithClose(catalog.create(sft, schemeOptions)) { metadata =>
          files.foreach(metadata.addFile)
          metadata.getFiles() mustEqual files.reverse
        }
      }
    }
    "return files based on partition" in {
      withPath { catalog =>
        WithClose(catalog.create(sft, schemeOptions)) { metadata =>
          files.foreach(metadata.addFile)
          metadata.getFiles() mustEqual files.reverse
          metadata.getFiles(partition1) mustEqual Seq(f1)
          metadata.getFiles(partition2) mustEqual Seq(f3, f2)
        }
      }
    }
    "return files based on filters" in {
      withPath { catalog =>
        WithClose(catalog.create(sft, schemeOptions)) { metadata =>
          files.foreach(metadata.addFile)
          metadata.getFiles() mustEqual files.reverse

          // date-only filters

          // filter for march 5th (should return f1)
          val filter1 = ECQL.toFilter("dtg >= '2026-03-05T00:00:00.000Z' AND dtg < '2026-03-06T00:00:00.000Z'")
          metadata.getFiles(filter1).map(_.file) mustEqual Seq(f1)

          // filter for march 4th (should return f2, f3)
          val filter2 = ECQL.toFilter("dtg >= '2026-03-04T00:00:00.000Z' AND dtg < '2026-03-05T00:00:00.000Z'")
          metadata.getFiles(filter2).map(_.file) mustEqual Seq(f3, f2)

          // filter for before march 4th (should return f5, f6)
          val filter3 = ECQL.toFilter("dtg < '2026-03-04T00:00:00.000Z'")
          metadata.getFiles(filter3).map(_.file) mustEqual Seq(f6, f5)

          // spatial-only filters (bbox format: bbox(geom, minx, miny, maxx, maxy))

          // northeast quadrant - should intersect with f1 bounds [1,2,11,12]
          val filter4 = ECQL.toFilter("BBOX(geom, 1, 1, 12, 13)")
          metadata.getFiles(filter4).map(_.file) mustEqual Seq(f1)

          // northwest quadrant - should intersect with f2, f3 bounds [-11,2,-1,12]
          val filter5 = ECQL.toFilter("BBOX(geom, -12, 1, -1, 13)")
          metadata.getFiles(filter5).map(_.file) mustEqual Seq(f3, f2)

          // southeast quadrant - should intersect with f5, f6 bounds [1,-12,11,-2]
          val filter6 = ECQL.toFilter("BBOX(geom, 1, -13, 12, -1)")
          metadata.getFiles(filter6).map(_.file) mustEqual Seq(f6, f5)

          // combined date and spatial filters

          // march 4th in northwest quadrant (should return f2, f3)
          val filter7 = ECQL.toFilter("dtg >= '2026-03-04T00:00:00.000Z' AND dtg < '2026-03-05T00:00:00.000Z' AND BBOX(geom, -12, 1, -1, 13)")
          metadata.getFiles(filter7).map(_.file) mustEqual Seq(f3, f2)

          // march 5th in northeast quadrant (should return f1)
          val filter8 = ECQL.toFilter("dtg >= '2026-03-05T00:00:00.000Z' AND BBOX(geom, 1, 1, 12, 13)")
          metadata.getFiles(filter8).map(_.file) mustEqual Seq(f1)

          // march 3rd in southeast quadrant (should return f5, f6)
          val filter9 = ECQL.toFilter("dtg >= '2026-03-03T00:00:00.000Z' AND dtg < '2026-03-04T00:00:00.000Z' AND BBOX(geom, 1, -13, 12, -1)")
          metadata.getFiles(filter9).map(_.file) mustEqual Seq(f6, f5)

          // date range spanning multiple days with spatial filter (march 4-5 in northeast should return f1 only)
          val filter10 = ECQL.toFilter("dtg >= '2026-03-04T00:00:00.000Z' AND BBOX(geom, 1, 1, 12, 13)")
          metadata.getFiles(filter10).map(_.file) mustEqual Seq(f1)

          // all march dates (should return all files)
          val filter11 = ECQL.toFilter("dtg >= '2026-03-01T00:00:00.000Z' AND dtg < '2026-04-01T00:00:00.000Z'")
          metadata.getFiles(filter11).map(_.file) mustEqual files.reverse
        }
      }
    }
//    "track modified and deleted files" in {
//      withPath { context =>
//        val f1 = StorageFile("file1", 0L)
//        val Seq(f2, f3) = Seq("file2", "file3").map(StorageFile(_, 1L))
//        val Seq(f5, f6) = Seq("file5", "file6").map(StorageFile(_, 2L))
//        val f5mod = StorageFile("file5", 3L, StorageFileAction.Delete)
//        val f2mod = StorageFile("file2", 3L, StorageFileAction.Modify)
//        val config = getConfig(context.root)
//        WithClose(factory.create(context, config, meta)) { created =>
//          created.addPartition(PartitionMetadata("1", Seq(f1), new Envelope(-10, 10, -5, 5), 10L))
//          created.addPartition(PartitionMetadata("1", Seq(f2, f3), new Envelope(-11, 11, -5, 5), 20L))
//          created.addPartition(PartitionMetadata("2", Seq(f5, f6), new Envelope(-1, 1, -5, 5), 20L))
//          created.addPartition(PartitionMetadata("2", Seq(f5mod), new Envelope(-1, 1, -5, 5), 20L))
//          created.addPartition(PartitionMetadata("1", Seq(f2mod), new Envelope(-11, 11, -5, 5), 20L))
//          val loaded = factory.load(context)
//          loaded must beSome
//          try {
//            foreach(Seq(created, loaded.get)) { metadata =>
//              metadata.encoding mustEqual encoding
//              metadata.sft mustEqual sft
//              metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
//              metadata.scheme mustEqual scheme
//              metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
//              metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f2, f3, f2mod)))
//              metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6,  f5mod)))
//            }
//          } finally {
//            loaded.get.close()
//          }
//        }
//      }
//    }
//    "delete and compact" in {
//      withPath { context =>
//        val config = getConfig(context.root)
//        WithClose(factory.create(context, config, meta)) { created =>
//          created.addPartition(PartitionMetadata("1", Seq(f1), new Envelope(-10, 10, -5, 5), 10L))
//          created.addPartition(PartitionMetadata("1", Seq(f2, f3), new Envelope(-11, 11, -5, 5), 20L))
//          created.addPartition(PartitionMetadata("2", Seq(f5, f6), new Envelope(-1, 1, -5, 5), 20L))
//          created.removePartition(PartitionMetadata("1", Seq(f2), new Envelope(-11, 11, -5, 5), 5L))
//          val loaded = factory.load(context)
//          loaded must beSome
//          try {
//            foreach(Seq(created, loaded.get)) { metadata =>
//              metadata.encoding mustEqual encoding
//              metadata.sft mustEqual sft
//              metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
//              metadata.scheme mustEqual scheme
//              metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
//              metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f3)))
//              metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6)))
//            }
//          } finally {
//            loaded.get.close()
//          }
//          created.compact(None)
//          created.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
//          created.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f3)))
//          created.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6)))
//        }
//      }
//    }
    "set and get key-value pairs" in {
      withPath { catalog =>
        WithClose(catalog.create(sft, schemeOptions)) { created =>
          created.set("foo", "bar")
          created.set("bar", "baz")
          WithClose(catalog.load(sft.getTypeName)) { loaded =>
            foreach(Seq(created, loaded)) { metadata =>
              metadata.sft mustEqual sft
              metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
              metadata.schemes mustEqual schemes
              metadata.get("foo") must beSome("bar")
              metadata.get("bar") must beSome("baz")
            }
          }
        }
      }
    }
//    "read old tables" in {
//      WithClose(getClass.getClassLoader.getResourceAsStream("jdbc/old_meta.sql")) { db =>
//        db must not(beNull)
//
//        withPath { context =>
//          val config = getConfig(context.root)
//          WithClose(JdbcMetadataFactory.createDataSource(config)) { source =>
//            WithClose(source.getConnection()) { connection =>
//              WithClose(connection.createStatement()) { statement =>
//                // splitting on ; may not be universally safe, but works for our script
//                IOUtils.toString(db, StandardCharsets.UTF_8).split(";").foreach { sql =>
//                  statement.execute(s"$sql;")
//                }
//              }
//              // update the root col in the metadata table to point to our current root
//              Seq("storage_meta", "storage_partitions", "storage_partition_files").foreach { table =>
//                WithClose(connection.prepareStatement(s"update $table set root = ?")) { ps =>
//                  ps.setString(1, context.root.toUri.toString)
//                  ps.executeUpdate()
//                }
//              }
//            }
//          }
//
//          // create the metadata.json file pointing to the table
//          MetadataJson.writeMetadata(context, NamedOptions(factory.name, config))
//
//          val loaded = factory.load(context)
//          loaded must beSome
//          WithClose(loaded.get) { metadata =>
//            metadata.encoding mustEqual encoding
//            metadata.sft mustEqual sft
//            metadata.sft.getUserData.asScala.toSeq must containAllOf(sft.getUserData.asScala.toSeq)
//            metadata.scheme mustEqual scheme
//            metadata.getPartitions().map(_.name) must containTheSameElementsAs(Seq("1", "2"))
//            metadata.getPartition("1").map(_.files) must beSome(containTheSameElementsAs(Seq(f1, f3).map(_.copy(timestamp = 0L))))
//            metadata.getPartition("2").map(_.files) must beSome(containTheSameElementsAs(Seq(f5, f6).map(_.copy(timestamp = 0L))))
//          }
//        }
//      }
//    }
  }

  def withPath[R](code: StorageMetadataCatalog => R): R = {
    val file = Files.createTempDirectory("geomesa").toFile.getPath
    try { code(newCatalog(FileSystemContext(fs, conf, new Path(file)))) } finally {
      FileUtils.deleteDirectory(new File(file))
    }
  }
}
