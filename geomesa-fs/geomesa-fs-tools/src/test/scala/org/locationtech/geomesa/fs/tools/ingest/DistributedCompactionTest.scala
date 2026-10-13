/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.iceberg.hadoop.HadoopCatalog
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, StorageCatalog}
import org.locationtech.geomesa.fs.tools.compact.FileSystemCompactionJob.ParquetCompactionJob
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}
import org.locationtech.geomesa.jobs.JobResult.{JobFailure, JobSuccess}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.file.Files

/** Supplies Hadoop configuration for a filesystem-only test catalog. */
class LocalCompactionCatalog extends HadoopCatalog {
  override def initialize(name: String, properties: java.util.Map[String, String]): Unit = {
    setConf(new Configuration())
    super.initialize(name, properties)
  }
}

class DistributedCompactionTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  sequential

  private def withStorage[T](test: (FileSystemStorage, Path) => T, partitions: Seq[String] = Seq.empty): T = {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("geomesa-compaction-test"))
    val directory = Files.createTempDirectory("geomesa-distributed-compaction")
    val config = Map(
      "catalog-impl" -> classOf[LocalCompactionCatalog].getName,
      "io-impl" -> "org.apache.iceberg.hadoop.HadoopFileIO",
      "warehouse" -> directory.resolve("warehouse").toUri.toString,
      "iceberg.namespace" -> "geomesa",
      "mapreduce.framework.name" -> "local",
      "fs.defaultFS" -> "file:///",
      "mapreduce.map.maxattempts" -> "1")
    try {
      WithClose(StorageCatalog(config)) { catalog =>
        val sft = SimpleFeatureTypes.createType("compact", "name:String,*geom:Point:srid=4326")
        WithClose(catalog.create(sft, partitions)) { storage =>
          test(storage, new Path(directory.resolve("results").toUri))
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

  private val status = new StatusCallback {
    override def apply(prefix: String, progress: Float, counters: Seq[(String, Long)], done: Boolean): Unit = {}
    override def reset(): Unit = {}
  }

  private def run(storage: FileSystemStorage, path: Path): JobResult =
    new ParquetCompactionJob().run(storage, storage.metadata.partitions(), Some(path), Seq.empty, Iterator.empty, status)

  "Distributed compaction" should {
    "stage worker outputs and publish exactly one replacement snapshot" in withStorage { (storage, path) =>
      append(storage, "one")
      append(storage, "two")
      val snapshot = storage.table.currentSnapshot().snapshotId()
      val inputs = storage.metadata.files().scan().map(_.file().location())
      run(storage, path) must beAnInstanceOf[JobSuccess]
      storage.table.refresh()
      storage.table.currentSnapshot().parentId().longValue() mustEqual snapshot
      storage.metadata.files().scan() must haveLength(1)
      storage.metadata.files().scan().head.file().recordCount() mustEqual 2L
      foreach(inputs)(input => storage.table.io().newInputFile(input).exists() must beTrue)
    }

    "publish all partition replacements in the same snapshot" in withStorage({ (storage, path) =>
      append(storage, "one-first", "one")
      append(storage, "one-second", "one")
      append(storage, "two-first", "two")
      append(storage, "two-second", "two")
      val snapshot = storage.table.currentSnapshot().snapshotId()
      storage.metadata.files().scan() must haveLength(4)
      run(storage, path) must beAnInstanceOf[JobSuccess]
      storage.table.refresh()
      storage.table.currentSnapshot().parentId().longValue() mustEqual snapshot
      val files = storage.metadata.files().scan()
      files must haveLength(2)
      foreach(files)(_.file().recordCount() mustEqual 2L)
    }, Seq("attribute:attribute=name"))

    "leave every partition registered when one mapper fails" in withStorage({ (storage, path) =>
      append(storage, "good")
      append(storage, "broken")
      val snapshot = storage.table.currentSnapshot().snapshotId()
      val inputs = storage.metadata.files().scan()
      val input = inputs.find(t => storage.metadata.partition(t.file()).values.exists(_.value == "broken")).get.file().location()
      storage.table.io().deleteFile(input)
      run(storage, path) must beAnInstanceOf[JobFailure]
      storage.table.refresh()
      storage.table.currentSnapshot().snapshotId() mustEqual snapshot
      storage.metadata.files().scan().map(_.file().location()).toSet mustEqual inputs.map(_.file().location()).toSet
    }, Seq("attribute:attribute=name"))
  }
}
