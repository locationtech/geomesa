/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.geotools.data.{DataStore, DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemFeatureStore
import org.locationtech.geomesa.fs.tools.compact.CompactCommand
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class CompactCommandTest extends Specification with BeforeAfterAll {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  sequential

  var cluster: MiniDFSCluster = _
  var directory: String = _
  val typeName = "orc"

  val tempDir: Path = Files.createTempDirectory("compactCommand")

  val pt: Point              = WKTUtils.read("POINT(0 0)").asInstanceOf[Point]
  val line: LineString       = WKTUtils.read("LINESTRING(0 0, 1 1, 4 4)").asInstanceOf[LineString]
  val polygon: Polygon       = WKTUtils.read("POLYGON((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11))").asInstanceOf[Polygon]
  val mpt: MultiPoint        = WKTUtils.read("MULTIPOINT((0 0), (1 1))").asInstanceOf[MultiPoint]
  val mline: MultiLineString = WKTUtils.read("MULTILINESTRING ((0 0, 1 1), \n  (2 2, 3 3))").asInstanceOf[MultiLineString]
  val mpolygon: MultiPolygon = WKTUtils.read("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11)))").asInstanceOf[MultiPolygon]

  val sft: SimpleFeatureType = SimpleFeatureTypes.createType(typeName, "name:String,age:Int,dtg:Date," +
    "*geom:MultiLineString:srid=4326,pt:Point,line:LineString,poly:Polygon,mpt:MultiPoint,mline:MultiLineString,mpoly:MultiPolygon")
  sft.setScheme("daily")

  val features: Seq[ScalaSimpleFeature] = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z", s"MULTILINESTRING((0 0, 10 10.$i))",
      pt, line, polygon, mpt, mline, mpolygon)
  }

  val features2: Seq[ScalaSimpleFeature] = Seq.tabulate(10) { j =>
    val i = j+10
    ScalaSimpleFeature.create(sft, s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (j % 3)}T04:03:02.0001Z", s"MULTILINESTRING((0 0, 10 10.$j))",
      pt, line, polygon, mpt, mline, mpolygon)
  }

  def getDataStore: DataStore = {
    val dsParams = Map(
      "fs.path" -> directory,
      "fs.encoding" -> typeName,
      "fs.config" -> "parquet.compression=gzip")
    DataStoreFinder.getDataStore(dsParams)
  }

  override def beforeAll(): Unit = {
    // Start MiniCluster
    val conf = new HdfsConfiguration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.toFile.getAbsolutePath)
    cluster = new MiniDFSCluster.Builder(conf).build()
    directory = cluster.getDataDirectory + s"_$typeName"
    val ds = getDataStore

    ds.createSchema(sft)

    writeFeature(ds, features)
    writeFeature(ds, features2)
  }

  def writeFeature(ds: DataStore, feats: Seq[ScalaSimpleFeature]): Unit = {
    WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
      feats.foreach { feature =>
        FeatureUtils.copyToWriter(writer, feature, useProvidedFid = true)
        writer.write()
      }
    }
  }

  "Compaction command" >> {
    "Before compacting should be multiple files" in {
      val ds = getDataStore

      val fs = ds.getFeatureSource(typeName)

      val iter = fs.getFeatures.features
      while (iter.hasNext) {
        val feat = iter.next
        feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
        featureHasProperGeometries(feat) mustEqual true
      }

      fs.getCount(Query.ALL) mustEqual 20
      fs.asInstanceOf[FileSystemFeatureStore].storage.metadata.getPartitions().map(_.files.size).sum mustEqual 6
    }

    "Compaction command should run successfully" in {
      val command = new CompactCommand()
      command.params.featureName = typeName
      command.params.path = directory
      command.params.mode = RunModes.Distributed
      command.execute()
      success
    }

    "After compacting should be fewer files" in {
      val ds = getDataStore
      val fs = ds.getFeatureSource(typeName)
      val metadata = fs.asInstanceOf[FileSystemFeatureStore].storage.metadata
      metadata.reload() // invalidate the cached values, which aren't updated by the compaction job

      val iter = fs.getFeatures.features
      while (iter.hasNext) {
        val feat = iter.next
        feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
        featureHasProperGeometries(feat) mustEqual true
      }
      fs.getCount(Query.ALL) mustEqual 20
      metadata.getPartitions().map(_.files.size).sum mustEqual 3
    }
  }

  def featureHasProperGeometries(sf: SimpleFeature): Boolean = {
    sf.getAttribute("pt").equals(pt) &&
    sf.getAttribute("line").equals(line) &&
    sf.getAttribute("poly").equals(polygon) &&
    sf.getAttribute("mpt").equals(mpt) &&
    sf.getAttribute("mline").equals(mline) &&
    sf.getAttribute("mpoly").equals(mpolygon)
  }

  override def afterAll(): Unit = {
    // Stop MiniCluster
    cluster.shutdown()
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
