/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import org.apache.commons.io.IOUtils
import org.geotools.api.data.{DataStore, Query, SimpleFeatureStore}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.memory.MemoryDataStore
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, Partition, StorageCatalog}
import org.locationtech.geomesa.fs.tools.ingest.container.FsContainerTest
import org.locationtech.geomesa.tools.`export`.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.io.FileInputStream
import java.net.URI
import java.nio.file.Files

class ExportToFsTest extends SpecificationWithJUnit with FsContainerTest {

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
          .addFeatures(new ListFeatureCollection(sft, features: _*))

      val context = FileSystemContext.create(URI.create(dsParams("fs.path")), configs)
      val file = Files.createTempFile("", "2016_01_01_out.parquet").toFile.getAbsolutePath

      val command: ExportCommand[DataStore] = new ExportCommand[DataStore]() {
        override val params: ExportParams = new ExportParams() {
          override def featureName: String = sft.getTypeName
        }
        override def connection: Map[String, String] = Map.empty
        override def loadDataStore(): DataStore = ds
      }
      command.params.file = file
      command.params.force = true
      command.execute()

      WithClose(StorageCatalog(context)) { catalog =>
        WithClose(catalog.create(sft, Seq("daily"))) { storage =>
          val register = URI.create("s3://geomesa/2016_01_01_out.parquet")
          WithClose(ObjectStore(context)) { fs =>
            WithClose(fs.create(register).get) { os =>
              WithClose(new FileInputStream(file)) { is =>
                IOUtils.copy(is, os)
              }
            }
          }
          storage.metadata.register(Map(Partition(storage.schemes.map(_.getPartition(features.head))) -> Seq(register)))

          val read = storage.getReader(new Query(sft.getTypeName), 1).map(ScalaSimpleFeature.copy).toList
          read mustEqual features
        }
      }
    }
  }
}
