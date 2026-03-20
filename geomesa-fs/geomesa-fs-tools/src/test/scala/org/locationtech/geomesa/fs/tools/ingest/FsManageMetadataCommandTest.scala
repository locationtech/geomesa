/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import org.apache.hadoop.fs.Path
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.concurrent.atomic.AtomicInteger

@RunWith(classOf[JUnitRunner])
class FsManageMetadataCommandTest extends Specification {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
  sft.setScheme("daily")

  val features =
    Seq(
      ScalaSimpleFeature.create(sft, "0", "name0", "2020-01-01T00:00:00.000Z", "POINT (0 0)"),
      ScalaSimpleFeature.create(sft, "1", "name2", "2021-01-01T00:00:00.000Z", "POINT (0 0)"),
      ScalaSimpleFeature.create(sft, "2", "name1", "2022-01-01T00:00:00.000Z", "POINT (0 0)")
    )

  val counter = new AtomicInteger(0)

  def nextPath(): String =
    s"${HadoopSharedCluster.Container.getHdfsUrl}/${getClass.getSimpleName}/${counter.incrementAndGet()}/"

  "ManageMetadata command" should {
    "find file inconsistencies" in {
      val dir = nextPath()
      val dsParams = Map("fs.path" -> dir, "fs.config.xml" -> HadoopSharedCluster.ContainerConfig, "fs.metadata.type" -> "file" )
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        ds.createSchema(SimpleFeatureTypes.copy(sft))
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
        val storage = ds.storage(sft.getTypeName)
        val files = storage.metadata.getFiles().map(_.file)
        files must haveLength(3)
        storage.context.fs.rename(new Path(storage.context.root, files.head), new Path(storage.context.root, files.head + ".bak"))
        // verify we can't retrieve the moved file
        val results = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
        results must haveLength(2)
        foreach(results)(f => features must contain(beEqualTo(f)))

        // TODO run the consistency check and repair any problems
//        val command = new FsManageMetadataCommand.CheckConsistencyCommand()
//        command.params.path = dir
//        command.params.featureName = sft.getTypeName
//        command.params.configuration = new java.util.ArrayList[(String, String)]()
//        HadoopSharedCluster.ContainerConfiguration.asScala.foreach(e => command.params.configuration.add(e.getKey -> e.getValue))
//        command.execute()
//
//        // verify the new file was registered and the old one removed
//        storage.metadata.invalidate()
//        storage.metadata.getPartitions().map(p => p.name -> p.files.length) must
//            containTheSameElementsAs(Seq("2020/01/01" -> 1, "2021/01/01" -> 1, "2019/01/01" -> 1))
//        storage.metadata.getPartitions().flatMap(_.files.map(_.name)) must containTheSameElementsAs(files)
//        // verify we can retrieve the moved file again
//        CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList must
//            containTheSameElementsAs(features)
      }
    }
  }
}
