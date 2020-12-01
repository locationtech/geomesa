/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.io.{FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent._
import java.util.function.BiFunction

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs._
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionMetadata, StorageFile}
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionScheme, StorageMetadata}
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.locationtech.geomesa.utils.text.StringSerialization
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.concurrent.ExecutionContext
import scala.runtime.BoxedUnit
import scala.util.control.NonFatal

/**
  * StorageMetadata implementation. Saves changes as a series of timestamped changelogs to allow
  * concurrent modifications. The current state is obtained by replaying all the logs.
  *
  * Note that state is not read off disk until 'reload' is called.
  *
  * When accessed through the standard factory methods, the state will periodically reload from disk
  * in order to pick up external changes (every 10 minutes by default).
  *
  * Note that modifications made to the metadata may not be immediately available, if they occur
  * simultaneously with a reload. For example, calling `getPartition` immediately after `addPartition` may
  * not return anything. However, the change is always persisted to disk, and will be available after the next
  * reload. In general this does not cause problems, as reads and writes happen in different JVMs (ingest
  * vs query).
  *
  * @param fc file context
  * @param directory metadata root path

  */
class RawDirectoryMetadata(
    private val fc: FileContext,
    val directory: Path,
    val sft: SimpleFeatureType,
    val encoding: String,
    val scheme: PartitionScheme,
    val leafStorage: Boolean
  ) extends StorageMetadata with MethodProfiling with LazyLogging {
  val partitionMetadata: PartitionMetadata = {
    val files = new mutable.ArrayBuffer[StorageFile]
    val iter = fc.listStatus(directory)
    while (iter.hasNext) {
      val status = iter.next()
      if (status.isFile) {
        println(s"Got a file: $status")
        files.append(StorageFile(status.getPath.toString, 0))
      }
    }
    // TODO: Get a count from the files...
    PartitionMetadata("raw", files.toSeq, None, 0)
  }

  override def getPartitions(prefix: Option[String]): Seq[PartitionMetadata] = Seq(partitionMetadata)

  override def getPartition(name: String): Option[PartitionMetadata] = Some(partitionMetadata)

  override def addPartition(partition: PartitionMetadata): Unit = { ??? }

  override def removePartition(partition: PartitionMetadata): Unit = { ??? }

  override def compact(partition: Option[String], threads: Int): Unit = { ??? }

  override def close(): Unit = {}
}

object RawDirectoryMetadata {
  val MetadataType = "raw"
}

