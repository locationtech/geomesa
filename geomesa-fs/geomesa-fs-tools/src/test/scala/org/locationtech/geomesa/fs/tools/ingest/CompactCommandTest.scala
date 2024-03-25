/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.HadoopSharedCluster
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

@RunWith(classOf[JUnitRunner])
class CompactCommandTest extends Specification {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  sequential

  val encodings = Seq("parquet", "orc")

  val pt       = WKTUtils.read("POINT(0 0)")
  val line     = WKTUtils.read("LINESTRING(0 0, 1 1, 4 4)")
  val polygon  = WKTUtils.read("POLYGON((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11))")
  val mpt      = WKTUtils.read("MULTIPOINT((0 0), (1 1))")
  val mline    = WKTUtils.read("MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))")
  val mpolygon = WKTUtils.read("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11)))")

  val sfts = encodings.map { name =>
    val sft = SimpleFeatureTypes.createType(name,
      "name:String,age:Int,dtg:Date," +
        "*geom:MultiLineString:srid=4326,pt:Point,line:LineString," +
          "poly:Polygon,mpt:MultiPoint,mline:MultiLineString,mpoly:MultiPolygon")
    sft.setEncoding(name)
    sft.setScheme("daily")
    sft
  }

  val numFeatures = 10000
  val targetFileSize = 8000L // kind of a magic number, in that it divides up the features into files fairly evenly with no remainder

  lazy val path = s"${HadoopSharedCluster.Container.getHdfsUrl}/${getClass.getSimpleName}/"

  lazy val ds = {
    val dsParams = Map(
      "fs.path" -> path,
      "fs.config.xml" -> HadoopSharedCluster.ContainerConfig
    )
    DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[FileSystemDataStore]
  }

  def features(sft: SimpleFeatureType): Seq[ScalaSimpleFeature] = {
    Seq.tabulate(numFeatures) { i =>
      ScalaSimpleFeature.create(sft,
        s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z", s"MULTILINESTRING((0 0, 10 10.${i % 10}))",
        pt, line, polygon, mpt, mline, mpolygon)
    }
  }

  step {
    sfts.foreach { sft =>
      ds.createSchema(sft)
      // create 2 files per partition
      features(sft).grouped(numFeatures / 2).foreach { feats =>
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          feats.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }
    }
  }

  "Compaction command" >> {
    "Before compacting should be multiple files per partition" in {
      foreach(sfts) { sft =>
        val fs = ds.getFeatureSource(sft.getTypeName)

        WithClose(fs.getFeatures.features) { iter =>
          while (iter.hasNext) {
            val feat = iter.next
            feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
            featureMustHaveProperGeometries(feat)
          }
        }

        fs.getCount(Query.ALL) mustEqual numFeatures
        ds.storage(sft.getTypeName).metadata.getPartitions().map(_.files.size) mustEqual Seq.fill(3)(2)
      }
    }

    "Compaction command should run successfully" in {
      foreach(sfts) { sft =>
        val command = new FsCompactCommand()
        command.params.featureName = sft.getTypeName
        command.params.path = path
        command.params.runMode = RunModes.Distributed.toString
        // invoke on our existing store so the cached metadata gets updated
        command.compact(ds)// must not(throwAn[Exception])
        ok
      }
    }

    "After compacting should be one file per partition" in {
      foreach(sfts) { sft =>
        val fs = ds.getFeatureSource(sft.getTypeName)

        WithClose(fs.getFeatures.features) { iter =>
          while (iter.hasNext) {
            val feat = iter.next
            feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
            featureMustHaveProperGeometries(feat)
          }
        }

        fs.getCount(Query.ALL) mustEqual numFeatures
        ds.storage(sft.getTypeName).metadata.getPartitions().map(_.files.size) mustEqual Seq.fill(3)(1)
      }
    }

    "Compaction command should run successfully with target file size" in {
      foreach(sfts) { sft =>
        val command = new FsCompactCommand()
        command.params.featureName = sft.getTypeName
        command.params.path = path
        command.params.runMode = RunModes.Distributed.toString
        command.params.targetFileSize = targetFileSize
        // invoke on our existing store so the cached metadata gets updated
        command.compact(ds) must not(throwAn[Exception])
      }
    }

    "After compacting with target file size should be multiple files per partition" in {
      foreach(sfts) { sft =>
        val fs = ds.getFeatureSource(sft.getTypeName)

        WithClose(fs.getFeatures.features) { iter =>
          while (iter.hasNext) {
            val feat = iter.next
            feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
            featureMustHaveProperGeometries(feat)
          }
        }

        fs.getCount(Query.ALL) mustEqual numFeatures
        val storage = ds.storage(sft.getTypeName)
        foreach(storage.metadata.getPartitions()) { partition =>
          partition.files.size must beGreaterThan(1)
          val sizes = storage.getFilePaths(partition.name).map(p => storage.context.fc.getFileStatus(p.path).getLen)
          // hard to get very close with 2 different formats and small files...
          foreach(sizes)(_ must beCloseTo(targetFileSize, 4000))
        }
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

  step {
    ds.dispose()
  }
}
