/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.iceberg.hadoop.HadoopCatalog
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, StorageCatalog}
import org.locationtech.geomesa.fs.tools.compact.FsCompactCommand
import org.locationtech.geomesa.fs.tools.ingest.LocalCompactionCommandTest.LocalCommandCompactionCatalog
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.tools.Command.CommandException
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.file.Files

class LocalCompactionCommandTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  sequential

  private def withStorage[T](test: (FileSystemStorage, Map[String, String]) => T, partitions: Seq[String] = Seq.empty): T = {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("geomesa-compaction-test"))
    val directory = Files.createTempDirectory("geomesa-local-compaction")
    val config = Map(
      "catalog-impl" -> classOf[LocalCommandCompactionCatalog].getName,
      "io-impl" -> "org.apache.iceberg.hadoop.HadoopFileIO",
      "warehouse" -> directory.resolve("warehouse").toUri.toString,
      "iceberg.namespace" -> "geomesa",
      "fs.defaultFS" -> "file:///")
    try {
      WithClose(StorageCatalog(config)) { catalog =>
        val sft = SimpleFeatureTypes.createType("compact", "name:String,*geom:Point:srid=4326")
        WithClose(catalog.create(sft, partitions)) { storage =>
          test(storage, config)
        }
      }
    } finally {
      WithClose(Files.walk(directory))(_.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists))
    }
  }

  private def append(storage: FileSystemStorage, id: String, name: String = null): Unit = {
    WithClose(storage.getMultiPartitionWriter()) { writer =>
      val feature = ScalaSimpleFeature.create(storage.sft, id, Option(name).getOrElse(id), "POINT (1 1)")
      SecurityUtils.setFeatureVisibility(feature, "A")
      writer.write(feature)
    }
  }

  private def run(config: Map[String, String]): Unit = {
    val command = new FsCompactCommand()
    command.params.featureName = "compact"
    command.params.configuration = config.toList.asJava
    command.params.threads = 2
    command.execute()
  }

  "Local compaction command" should {
    "compact all partitions in local mode and retain their rows" in withStorage({ (storage, config) =>
      append(storage, "one-first", "one")
      append(storage, "one-second", "one")
      append(storage, "two-first", "two")
      append(storage, "two-second", "two")
      val inputs = storage.metadata.files().scan().map(_.file().location())
      inputs must haveLength(4)
      run(config)
      storage.table.refresh()
      val files = storage.metadata.files().scan()
      files must haveLength(2)
      foreach(files)(_.file().recordCount() mustEqual 2L)
      foreach(inputs)(input => storage.table.io().newInputFile(input).exists() must beTrue)
    }, Seq("attribute:attribute=name"))

    "report reader failures and leave the failed partition registered" in withStorage { (storage, config) =>
      append(storage, "broken")
      val snapshot = storage.table.currentSnapshot().snapshotId()
      val inputs = storage.metadata.files().scan()
      storage.table.io().deleteFile(inputs.head.file().location())
      run(config) must throwA[CommandException]
      storage.table.refresh()
      storage.table.currentSnapshot().snapshotId() mustEqual snapshot
      storage.metadata.files().scan().map(_.file().location()).toSet mustEqual inputs.map(_.file().location()).toSet
    }

    "succeed for an empty table" in withStorage { (storage, config) =>
      run(config) must not(throwAn[Exception])
      storage.table.refresh()
      storage.metadata.files().scan() must beEmpty
    }
  }
}

object LocalCompactionCommandTest {

  /** Supplies Hadoop configuration for a filesystem-only test catalog. */
  class LocalCommandCompactionCatalog extends HadoopCatalog {
    override def initialize(name: String, properties: java.util.Map[String, String]): Unit = {
      setConf(new Configuration())
      super.initialize(name, properties)
    }
  }
}
