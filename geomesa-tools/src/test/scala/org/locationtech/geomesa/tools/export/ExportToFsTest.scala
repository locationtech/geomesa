/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io._
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.memory.MemoryDataStore
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, Query}
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionMetadata, StorageFile}
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, Metadata, NamedOptions}
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataFactory
import org.locationtech.geomesa.parquet.ParquetFileSystemStorageFactory
import org.locationtech.geomesa.tools.DataStoreRegistration
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExportToFsTest extends Specification {

  private val counter = new AtomicInteger(0)

  var out: java.nio.file.Path = _

  step {
    out = Files.createTempDirectory("gm-export-fs-test")
  }

  "Export command" should {
    "create files readable by the FSDS" >> {
      val sft = SimpleFeatureTypes.createType("tools", "name:String,dtg:Date,*geom:Point:srid=4326")
      val features = List(
        ScalaSimpleFeature.create(sft, "id1", "name1", "2016-01-01T01:00:00.000Z", "POINT(1 0)"),
        ScalaSimpleFeature.create(sft, "id2", "name2", "2016-01-01T02:00:00.000Z", "POINT(0 2)")
      )
      features.foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))

      val ds = new MemoryDataStore() {
        override def dispose(): Unit = {} // prevent dispose from deleting our data
      }
      ds.createSchema(sft)
      ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features.toArray[SimpleFeature]))

      val storage = {
        val context = FileSystemContext(FileContext.getFileContext, new Configuration(), new Path(out.toUri))
        val metadata =
          new FileBasedMetadataFactory()
              .create(context, Map.empty, Metadata(sft, "parquet", NamedOptions("daily"), leafStorage = true))
        new ParquetFileSystemStorageFactory().apply(context, metadata)
      }

      val file = new File(s"$out/2016/01/01_out.parquet")

      WithClose(storage) { storage =>
        val key = s"${getClass.getName}:${counter.getAndIncrement()}"
        try {
          DataStoreRegistration.register(key, ds)
          val command: ExportCommand[DataStore] = new ExportCommand[DataStore]() {
            override val params: ExportParams = new ExportParams() {
              override def featureName: String = sft.getTypeName
            }
            override def connection: Map[String, String] = Map(DataStoreRegistration.param.key -> key)
          }
          command.params.file = file.getAbsolutePath
          command.execute()
        } finally {
          DataStoreRegistration.unregister(key, ds)
        }

        storage.metadata.addPartition(PartitionMetadata("2016/01/01", Seq(StorageFile(file.getName, 0L)), None, 2L))

        val read = WithClose(storage.getReader(new Query(sft.getTypeName)))(_.toList)
        read mustEqual features
      }
    }
  }

  step {
    PathUtils.deleteRecursively(out)
  }
}
