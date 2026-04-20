/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.compact.FsCompactCommand
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom._
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll


@RunWith(classOf[JUnitRunner])
class CompactCommandTest extends Specification with BeforeAfterAll {

  import org.locationtech.geomesa.fs.storage.core.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  sequential

  val pt       = WKTUtils.read("POINT(0 0)")
  val line     = WKTUtils.read("LINESTRING(0 0, 1 1, 4 4)")
  val polygon  = WKTUtils.read("POLYGON((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11))")
  val mpt      = WKTUtils.read("MULTIPOINT((0 0), (1 1))")
  val mline    = WKTUtils.read("MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))")
  val mpolygon = WKTUtils.read("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11)))")

  val sft = SimpleFeatureTypes.createType("parquet",
    "name:String,age:Int,dtg:Date," +
      "*geom:MultiLineString:srid=4326,pt:Point,line:LineString," +
        "poly:Polygon,mpt:MultiPoint,mline:MultiLineString,mpoly:MultiPolygon")
  sft.setScheme("daily")

  val numFeatures = 10000
  val targetFileSize = 35000L // kind of a magic number, in that it divides up the features into files fairly evenly with no remainder

  lazy val path = s"${HadoopSharedCluster.Container.getHdfsUrl}/${getClass.getSimpleName}/"

  lazy val params = Map(
    "fs.path" -> path,
    "fs.metadata.type" -> "file",
    "fs.config.xml" ->
      HadoopSharedCluster.ContainerConfig
        .replaceFirst("<configuration>",
          "<configuration><property><name>geomesa.parquet.bounding-boxes</name><value>false</value></property>")
  )

  def features(sft: SimpleFeatureType): Seq[ScalaSimpleFeature] = {
    Seq.tabulate(numFeatures) { i =>
      ScalaSimpleFeature.create(sft,
        s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z", s"MULTILINESTRING((0 0, 10 10.${i % 10}))",
        pt, line, polygon, mpt, mline, mpolygon)
    }
  }

  override def beforeAll(): Unit = {
    WithClose(DataStoreFinder.getDataStore(params.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
      ds.createSchema(sft)
      // create 2 files per partition
      features(sft).grouped(numFeatures / 2).foreach { feats =>
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          feats.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }
    }
  }

  override def afterAll(): Unit = {}

  "Compaction command" should {
    "be multiple files per partition before compacting" in {
      WithClose(DataStoreFinder.getDataStore(params.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        val fs = ds.getFeatureSource(sft.getTypeName)
        WithClose(fs.getFeatures.features) { iter =>
          while (iter.hasNext) {
            val feat = iter.next
            feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
            featureMustHaveProperGeometries(feat)
          }
        }
        fs.getCount(Query.ALL) mustEqual numFeatures
        ds.storage(sft.getTypeName).metadata.getFiles() must haveLength(6)
      }
    }

    "run successfully" in {
      val command = new FsCompactCommand()
      command.params.featureName = sft.getTypeName
      command.params.path = path
      command.params.metadataType = "file"
      command.params.runMode = RunModes.Distributed.toString
      command.execute() must not(throwAn[Exception])
    }

    "be one file per partition after compacting" in {
      WithClose(DataStoreFinder.getDataStore(params.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        ds.storage(sft.getTypeName).metadata.getFiles() must haveLength(3)

        val fs = ds.getFeatureSource(sft.getTypeName)
        WithClose(fs.getFeatures.features) { iter =>
          while (iter.hasNext) {
            val feat = iter.next
            feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
            featureMustHaveProperGeometries(feat)
          }
        }
        fs.getCount(Query.ALL) mustEqual numFeatures
      }
    }

    "run successfully with target file size" in {
      val command = new FsCompactCommand()
      command.params.featureName = sft.getTypeName
      command.params.path = path
      command.params.metadataType = "file"
      command.params.runMode = RunModes.Distributed.toString
      command.params.targetFileSize = targetFileSize
      command.execute() must not(throwAn[Exception])
    }

    "be multiple files per partition after compacting with target file size" in {
      WithClose(DataStoreFinder.getDataStore(params.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        val storage = ds.storage(sft.getTypeName)
        foreach(storage.metadata.getFiles().groupBy(_.partition).values) { partition =>
          partition.size must beGreaterThan(1)
          val sizes = partition.map(f => storage.context.root.resolve(f.file)).map(p => storage.context.fs.size(p))
          // hard to get very close with small files...
          foreach(sizes)(_ must beCloseTo(targetFileSize, 6000))
        }

        val fs = ds.getFeatureSource(sft.getTypeName)
        WithClose(fs.getFeatures.features) { iter =>
          while (iter.hasNext) {
            val feat = iter.next
            feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
            featureMustHaveProperGeometries(feat)
          }
        }
        fs.getCount(Query.ALL) mustEqual numFeatures
      }
    }
  }

  def featureMustHaveProperGeometries(sf: SimpleFeature): MatchResult[Any] = {
    sf.getAttribute("pt") mustEqual pt
    sf.getAttribute("line") mustEqual line
    sf.getAttribute("poly") mustEqual polygon
    sf.getAttribute("mpt") mustEqual mpt
    sf.getAttribute("mline") mustEqual mline
    sf.getAttribute("mpoly") mustEqual mpolygon
  }
}
