/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package metadata

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.{AttributeBounds, SpatialBounds, StorageFile}
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification

import java.net.URI
import java.nio.file.Files

abstract class TestAbstractMetadata extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  lazy val fs = LocalObjectStore
  val sft = SimpleFeatureTypes.createType("metadata",
    "name:String:fs.bounds=true,dtg:Date,*geom:Point:srid=4326;geomesa.user-data.prefix=desc,desc.name=姓名,desc.dtg=ひづけ,desc.geom=좌표")
  val encoding = "parquet"
  val schemeOptions = Seq("hour", "z2:bits=2")
  val schemes = schemeOptions.map(PartitionSchemeFactory.load(sft, _)).toSet

  // note: ensure that partitions and bounds line up
  val partition1 = partitions("2026-03-05T00:00:00.000Z", "POINT (10 10)") // dtg 8000019cbb4b4c00/8000019cbb823a80
  // note: the second bounds doesn't match the sft, but it validates persisting multiple spatial bounds
  val spatialBounds1 = Seq(SpatialBounds(2, 1, 2, 11, 12), SpatialBounds(4, -1, -2, -11, -12))
  val attributeBounds1 = Seq(AttributeBounds(0, "name5", "name6"), AttributeBounds(1, "8000019cbb4b4c00", "8000019cbb823a80"))

  val partition2 = partitions("2026-03-04T00:00:00.000Z", "POINT (-10 10)") // dtg 8000019cb624f000/8000019cb65bde80
  val spatialBounds2a = Seq(SpatialBounds(2, -11, 2, -1, 12))
  val spatialBounds2b = Seq(SpatialBounds(2, -20, 12, -12, 20))
  val attributeBounds2 = Seq(AttributeBounds(0, "name0", "name4"), AttributeBounds(1, "8000019cb624f000", "8000019cb6406740")) // first 30 minutes

  val partition3 = partitions("2026-03-03T00:00:00.000Z", "POINT (10 -10)") // dtg 8000019cb0fe9400/8000019cb1358280
  val spatialBounds3 = Seq(SpatialBounds(2, 1, -12, 11, -2))
  val attributeBounds3a = Seq(AttributeBounds(0, "name7", "name9"), AttributeBounds(1, "8000019cb0fe9400", "8000019cb11a0b40")) // first 30 minutes
  val attributeBounds3b = Seq(AttributeBounds(0, "name7", "name9"), AttributeBounds(1, "8000019cb11a0b40", "8000019cb1358280")) // last 30 minutes

  val f1 = StorageFile("file1", partition1, 0L, spatialBounds = spatialBounds1, attributeBounds = attributeBounds1, timestamp = System.currentTimeMillis() - 100)
  val f2a = StorageFile("file2a", partition2, 1L, spatialBounds = spatialBounds2a, attributeBounds = attributeBounds2, timestamp = f1.timestamp + 10)
  val f2b = StorageFile("file2b", partition2, 1L, spatialBounds = spatialBounds2b, attributeBounds = attributeBounds2, timestamp = f2a.timestamp + 10)
  val f3a = StorageFile("file3a", partition3, 2L, spatialBounds = spatialBounds3, attributeBounds = attributeBounds3a, timestamp = f2b.timestamp + 20)
  val f3b = StorageFile("file3b", partition3, 2L,  spatialBounds = spatialBounds3, attributeBounds = attributeBounds3b, timestamp = f3a.timestamp + 10)

  val files = Seq(f1, f2a, f2b, f3a, f3b)

  private def partitions(dtg: String, geom: String): Partition = {
    val sf = ScalaSimpleFeature.create(sft, "", "", dtg, geom)
    Partition(schemes.map(_.getPartition(sf)))
  }

  protected def metadataType: String

  protected def getConfig(root: URI): Map[String, String]

  def newCatalog(root: URI) = {
    val conf = getConfig(root)
    StorageMetadataCatalog(FileSystemContext(fs, root, conf), metadataType, conf)
  }

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
    "return files based on partition" in {
      withPath { catalog =>
        WithClose(catalog.create(sft, schemeOptions)) { metadata =>
          files.foreach(metadata.addFile)
          metadata.getFiles() mustEqual files.reverse
          metadata.getFiles(partition1) mustEqual Seq(f1)
          metadata.getFiles(partition2) mustEqual Seq(f2b, f2a)
        }
      }
    }
    "return files based on filters" in {
      withPath { catalog =>
        WithClose(catalog.create(sft, schemeOptions)) { metadata =>

          files.foreach(metadata.addFile)
          metadata.getFiles() mustEqual files.reverse

          val expectations = Seq(
            // date-only filters
            "dtg >= '2026-03-05T00:00:00.000Z' AND dtg < '2026-03-06T00:00:00.000Z'" -> Seq(f1),
            "dtg >= '2026-03-04T00:00:00.000Z' AND dtg < '2026-03-05T00:00:00.000Z'" -> Seq(f2b, f2a),
            "dtg < '2026-03-04T00:00:00.000Z'" -> Seq(f3b, f3a),
            // this should filter not just on partition but also on attribute bounds
            "dtg >= '2026-03-03T00:00:00.000Z' AND dtg < '2026-03-03T00:29:00.000Z'" -> Seq(f3a),
            // OR
            "(dtg >= '2026-03-03T00:00:00.000Z' AND dtg < '2026-03-03T00:29:00.000Z') OR (dtg >= '2026-03-05T00:00:00.000Z' AND dtg < '2026-03-06T00:00:00.000Z')" -> Seq(f3a, f1),
            "dtg >= '2026-03-01T00:00:00.000Z' AND dtg < '2026-04-01T00:00:00.000Z'" -> files.reverse,
            // spatial-only filters (bbox format: bbox(geom, minx, miny, maxx, maxy))
            // northeast quadrant - should intersect with [1,2,11,12]
            "BBOX(geom, 1, 1, 12, 13)" -> Seq(f1),
            // northwest quadrant - should intersect with [-11,2,-1,12]
            "BBOX(geom, -12, 1, -1, 13)" -> Seq(f2b, f2a),
            // this should filter not just on partition but also on spatial bounds
            "BBOX(geom, -12, 1, -1, 11)" -> Seq(f2a),
            // southeast quadrant - should intersect with [1,-12,11,-2]
            "BBOX(geom, 1, -13, 12, -1)" -> Seq(f3b, f3a),
            // combined date and spatial filters
            // march 4th in northwest quadrant
            "dtg >= '2026-03-04T00:00:00.000Z' AND dtg < '2026-03-05T00:00:00.000Z' AND BBOX(geom, -12, 1, -1, 13)" -> Seq(f2b, f2a),
            // march 5th in northeast quadrant
            "dtg >= '2026-03-05T00:00:00.000Z' AND BBOX(geom, 1, 1, 12, 13)" -> Seq(f1),
            // march 3rd in southeast quadrant
            "dtg >= '2026-03-03T00:00:00.000Z' AND dtg < '2026-03-04T00:00:00.000Z' AND BBOX(geom, 1, -13, 12, -1)" -> Seq(f3b, f3a),
            // date range spanning multiple days with spatial filter (march 4-5 in northeast)
            "dtg >= '2026-03-04T00:00:00.000Z' AND BBOX(geom, 1, 1, 12, 13)" -> Seq(f1),
            // complex, multi-join filter
            "(dtg >= '2026-03-03T00:31:00.000Z' AND dtg < '2026-03-03T00:59:00.000Z') OR (dtg >= '2026-03-05T00:00:00.000Z' AND dtg < '2026-03-06T00:00:00.000Z') AND BBOX(geom, 1, -13, 12, -1)" -> Seq(f3b),
            // non-partition based attribute filter
            "dtg >= '2026-03-01T00:00:00.000Z' AND dtg < '2026-04-01T00:00:00.000Z' AND name = 'name8'" -> Seq(f3b, f3a),
          )

          foreach(expectations) { case (ecql, expected) =>
            metadata.getFiles(ECQL.toFilter(ecql)) mustEqual expected
          }
        }
      }
    }
    "delete files" in {
      withPath { catalog =>
        WithClose(catalog.create(sft, schemeOptions)) { metadata =>
          files.foreach(metadata.addFile)
          metadata.getFiles() mustEqual files.reverse
          metadata.getFiles(partition1) mustEqual Seq(f1)
          metadata.getFiles(partition2) mustEqual Seq(f2b, f2a)
          metadata.removeFile(f2a)
          metadata.getFiles() mustEqual files.reverse.filter(_ != f2a)
          metadata.getFiles(partition1) mustEqual Seq(f1)
          metadata.getFiles(partition2) mustEqual Seq(f2b)
        }
      }
    }
  }

  def withPath[R](code: StorageMetadataCatalog => R): R = {
    val file = Files.createTempDirectory("geomesa").toFile
    try { code(newCatalog(file.toURI)) } finally {
      FileUtils.deleteDirectory(file)
    }
  }
}
