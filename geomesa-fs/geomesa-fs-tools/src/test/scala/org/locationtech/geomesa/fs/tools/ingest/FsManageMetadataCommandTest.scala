/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import org.apache.hadoop.fs.Path
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.HadoopSharedCluster
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

@RunWith(classOf[JUnitRunner])
class FsManageMetadataCommandTest extends Specification {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
  sft.setEncoding("parquet")
  sft.setScheme("daily")

  val features =
    Seq(
      ScalaSimpleFeature.create(sft, "0", "name0", "2020-01-01T00:00:00.000Z", "POINT (0 0)"),
      ScalaSimpleFeature.create(sft, "1", "name2", "2021-01-01T00:00:00.000Z", "POINT (0 0)"),
      ScalaSimpleFeature.create(sft, "2", "name1", "2022-01-01T00:00:00.000Z", "POINT (0 0)")
    )

  val gzipXml =
    "<configuration><property><name>parquet.compression</name><value>GZIP</value></property></configuration>"

  val counter = new AtomicInteger(0)

  def nextPath(): String =
    s"${HadoopSharedCluster.Container.getHdfsUrl}/${getClass.getSimpleName}/${counter.incrementAndGet()}/"

  "ManageMetadata command" should {
    "find file inconsistencies" in {
      val dir = nextPath()
      val dsParams = Map("fs.path" -> dir, "fs.config.xml" -> gzipXml)
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        ds.createSchema(SimpleFeatureTypes.copy(sft))
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
        val storage = ds.storage(sft.getTypeName)
        storage.metadata.getPartitions().map(p => p.name -> p.files.length) must
            containTheSameElementsAs(Seq("2020/01/01" -> 1, "2021/01/01" -> 1, "2022/01/01" -> 1))
        val files = storage.metadata.getPartitions().flatMap(_.files.map(_.name)).toList
        // move a file - it's not in the right partition so it won't be matched correctly by filters,
        // but it's good enough for a test
        storage.context.fc.rename(new Path(storage.context.root, "2022"), new Path(storage.context.root, "2019"))
        // verify we can't retrieve the moved file
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(features.take(2))

        // run the consistency check and repair any problems
        val command = new FsManageMetadataCommand.CheckConsistencyCommand()
        command.params.path = dir
        command.params.featureName = sft.getTypeName
        command.params.configuration = Collections.singletonList(s"fs.config.xml=$gzipXml")
        command.params.repair = true
        command.execute()

        // verify the new file was registered and the old one removed
        storage.metadata.invalidate()
        storage.metadata.getPartitions().map(p => p.name -> p.files.length) must
            containTheSameElementsAs(Seq("2020/01/01" -> 1, "2021/01/01" -> 1, "2019/01/01" -> 1))
        storage.metadata.getPartitions().flatMap(_.files.map(_.name)) must containTheSameElementsAs(files)
        // verify we can retrieve the moved file again
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(features)
      }
    }
    "rebuild metadata from scratch" in {
      val dir = nextPath()
      val dsParams = Map("fs.path" -> dir, "fs.config.xml" -> gzipXml)
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        ds.createSchema(SimpleFeatureTypes.copy(sft))
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
        val storage = ds.storage(sft.getTypeName)
        // delete a file
        storage.context.fc.delete(new Path(storage.context.root, "2022"), true)
        // delete a partition from the metadata
        storage.metadata.getPartition("2021/01/01") match {
          case None => ko("Expected Some for partition but got none")
          case Some(p) => storage.metadata.removePartition(p)
        }
        // note that one reference is invalid
        storage.metadata.getPartitions().map(p => p.name -> p.files.length) must
            containTheSameElementsAs(Seq("2022/01/01" -> 1, "2020/01/01" -> 1))
        // verify we can only retrieve one feature
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList mustEqual
            features.take(1)

        // run the consistency check and rebuild the metadata
        val command = new FsManageMetadataCommand.CheckConsistencyCommand()
        command.params.path = dir
        command.params.featureName = sft.getTypeName
        command.params.configuration = Collections.singletonList(s"fs.config.xml=$gzipXml")
        command.params.rebuild = true
        command.execute()

        // verify the new file was registered and the old one removed
        storage.metadata.invalidate()
        storage.metadata.getPartitions().map(p => p.name -> p.files.length) must
            containTheSameElementsAs(Seq("2020/01/01" -> 1, "2021/01/01" -> 1))
        // verify we can retrieve the moved file again
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList must
            containTheSameElementsAs(features.take(2))
      }
    }
  }
}
