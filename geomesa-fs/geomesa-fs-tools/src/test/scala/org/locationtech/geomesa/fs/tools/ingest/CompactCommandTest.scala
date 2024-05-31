/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.HadoopSharedCluster
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema.GeoParquetSchemaKey
import org.locationtech.geomesa.fs.tools.compact.FsCompactCommand
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.nio.file.Files
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class CompactCommandTest extends Specification {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  sequential

  val encodings = Seq("parquet", "orc")

  val tempDir: java.nio.file.Path = Files.createTempDirectory("compactCommand")

  val sfts: java.util.Map[String, SimpleFeatureType] = {
    def createSft(encoding: String): SimpleFeatureType = {
      val sft = SimpleFeatureTypes.createType(encoding,
        "name:String,age:Int,dtg:Date," +
          "*geom:MultiLineString:srid=4326,pt:Point,line:LineString," +
          "poly:Polygon,mpt:MultiPoint,mline:MultiLineString,mpoly:MultiPolygon")
      sft.setEncoding(encoding)
      sft.setScheme("daily")
      sft
    }

    val map = Map[String, SimpleFeatureType](
      encodings.head -> createSft(encodings.head),
      encodings(1) -> createSft(encodings(1))
    ).asJava

    map
  }

  val numFeatures = 10000

  // kind of a magic number, in that it divides up the features into files fairly evenly with no remainder
  val targetFileSize: java.util.Map[String, Long] = Map[String, Long](
    encodings.head -> 15000L,
    encodings(1) -> 14000L
  ).asJava

  lazy val path = s"${HadoopSharedCluster.Container.getHdfsUrl}/${getClass.getSimpleName}/"

  lazy val ds = {
    val dsParams = Map(
      "fs.path" -> path,
      "fs.config.xml" -> HadoopSharedCluster.ContainerConfig
    )
    DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[FileSystemDataStore]
  }

  // A map between partition name and a set of bounding boxes of each file in that partition
  val partitionBoundingBoxes = new mutable.HashMap[String, mutable.Set[Envelope]] with mutable.MultiMap[String, Envelope]

  def features(sft: SimpleFeatureType): Seq[ScalaSimpleFeature] = {
    (0 until numFeatures).map { i =>
      val sf = new ScalaSimpleFeature(sft, i.toString)
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      sf.setAttribute(0, s"name${i % 10}")
      sf.setAttribute(1, s"${i % 10}")
      sf.setAttribute(2, f"2014-01-${i % 10 + 1}%02dT00:00:01.000Z")
      sf.setAttribute(3, s"MULTILINESTRING((0 0, 10 10.${i % 10}))")
      sf.setAttribute(4, s"POINT(4${i % 10} 5${i % 10})")
      sf.setAttribute(5, s"LINESTRING(0 0, $i $i, 4 4)")
      sf.setAttribute(6, s"POLYGON((${i % 10} ${i % 10}, ${i % 10} ${i % 20}, ${i % 20} ${i % 20}, ${i % 20} ${i % 10}, ${i % 10} ${i % 10}), (${i % 11} ${i % 11}, ${i % 19} ${i % 11}, ${i % 19} ${i % 19}, ${i % 11} ${i % 19}, ${i % 11} ${i % 11}))")
      sf.setAttribute(7, s"MULTIPOINT((0 0), ($i $i))")
      sf.setAttribute(8, s"MULTILINESTRING ((0 0, ${(i+1) % 10} ${(i+1) % 10}), (${(2*i+1) % 10} ${(2*i+1) % 10}, ${(3*i+1) % 10} ${(3*i+1) % 10}))")
      sf.setAttribute(9, s"MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11)))")
      sf
    }
  }

  // Helper for extracting a bounding box from GeoParquet metadata
  def getBoundingBoxFromGeoParquetFile(path: Path): Envelope = {
    val conf = new Configuration()
    val footer = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER)
    val metadata = footer.getFileMetaData.getKeyValueMetaData.get(GeoParquetSchemaKey)

    val start = metadata.indexOf("bbox") + 7
    val end = metadata.indexOf("]", start)
    val coordinates = metadata.substring(start, end).split(',').map(_.trim.toDouble)

    val x1 = coordinates(0)
    val x2 = coordinates(1)
    val y1 = coordinates(2)
    val y2 = coordinates(3)
    new Envelope(x1, x2, y1, y2)
  }

  val numFilesPerPartition = 2

  step {
    encodings.foreach(encoding => {
      val sft = sfts.get(encoding)
      ds.createSchema(sft)

      features(sft).grouped(numFeatures / numFilesPerPartition).foreach { feats =>
        val writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        WithClose(writer) { writer =>
          feats.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }
    })
  }

  "Compaction command" >> {
    "Before compacting should be multiple files per partition" in {
      foreach(encodings) { encoding =>
        val sft = sfts.get(encoding)
        val fs = ds.getFeatureSource(sft.getTypeName)

        WithClose(fs.getFeatures.features) { iter =>
          while (iter.hasNext) {
            val feat = iter.next
            feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
          }
        }

        fs.getCount(Query.ALL) mustEqual numFeatures

        val partitions = ds.storage(sft.getTypeName).metadata.getPartitions()

        // For parquet files, get bounding boxes from each file in each partition
        if (encoding == "parquet") {
          val partitionNames = partitions.map(_.name)
          partitionNames.foreach(partitionName => {
            val filePaths = ds.storage(sft.getTypeName).getFilePaths(partitionName)
            filePaths.foreach(path => {
              val filepath = path.path
              val bbox = getBoundingBoxFromGeoParquetFile(filepath)
              partitionBoundingBoxes.addBinding(partitionName, bbox)
            })
          })
        }

        partitions.map(_.files.size) mustEqual Seq.fill(10)(numFilesPerPartition)
      }
    }

    "Compaction command should run successfully" in {
      foreach(encodings) { encoding =>
        val sft = sfts.get(encoding)
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
      foreach(encodings) { encoding =>
        val sft = sfts.get(encoding)
        val fs = ds.getFeatureSource(sft.getTypeName)

        WithClose(fs.getFeatures.features) { iter =>
          while (iter.hasNext) {
            val feat = iter.next
            feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
          }
        }

        fs.getCount(Query.ALL) mustEqual numFeatures

        val partitions = ds.storage(sft.getTypeName).metadata.getPartitions()

        // For parquet files, check that the union of bounding boxes of the 2 files before
        // compaction is the same as the bounding box of the 1 file after compaction
        if (encoding == "parquet") {
          val partitionNames = partitions.map(_.name)
          partitionNames.foreach(partitionName => {
            val filePaths = ds.storage(sft.getTypeName).getFilePaths(partitionName).map(_.path)
            filePaths.foreach(path => {
                // In each partition, assert that the
                val bboxesUnion = new Envelope
                partitionBoundingBoxes(partitionName).foreach(bbox => bboxesUnion.expandToInclude(bbox))
                val metadataBbox = getBoundingBoxFromGeoParquetFile(path)
                bboxesUnion mustEqual metadataBbox
            })
          })
        }

        partitions.map(_.files.size) mustEqual Seq.fill(10)(1)
      }
    }

    "Compaction command should run successfully with target file size" in {
      foreach(encodings) { encoding =>
        val sft = sfts.get(encoding)
        val command = new FsCompactCommand()
        command.params.featureName = sft.getTypeName
        command.params.path = path
        command.params.runMode = RunModes.Distributed.toString
        command.params.targetFileSize = targetFileSize.get(encoding)
        // invoke on our existing store so the cached metadata gets updated
        command.compact(ds) must not(throwAn[Exception])
      }
    }

    "After compacting with target file size should be multiple files per partition" in {
      foreach(encodings) { encoding =>
        val sft = sfts.get(encoding)
        val fs = ds.getFeatureSource(sft.getTypeName)

        WithClose(fs.getFeatures.features) { iter =>
          while (iter.hasNext) {
            val feat = iter.next
            feat.getDefaultGeometry.asInstanceOf[MultiLineString].isEmpty mustEqual false
          }
        }

        fs.getCount(Query.ALL) mustEqual numFeatures
        val storage = ds.storage(sft.getTypeName)
        foreach(storage.metadata.getPartitions()) { partition =>
          partition.files.size must beGreaterThan(1)
          val sizes = storage.getFilePaths(partition.name).map(p => storage.context.fc.getFileStatus(p.path).getLen)
          // hard to get very close with 2 different formats and small files...
          foreach(sizes)(size => {
            size must beCloseTo(targetFileSize.get(encoding), 12000)
          })
        }
      }
    }
  }

  step {
    ds.dispose()
  }
}
