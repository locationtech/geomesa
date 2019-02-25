/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent._

import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs._
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionMetadata
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionScheme, StorageMetadata}
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.SimpleFeatureType
import pureconfig.ConfigWriter

import scala.collection.parallel.ExecutionContextTaskSupport
import scala.concurrent.ExecutionContext
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
  * @param sft simple feature type
  * @param encoding file encoding
  * @param scheme partition scheme
  * @param leafStorage leaf storage
  */
class FileBasedMetadata(
    fc: FileContext,
    directory: Path,
    val sft: SimpleFeatureType,
    val encoding: String,
    val scheme: PartitionScheme,
    val leafStorage: Boolean
  ) extends StorageMetadata {

  import scala.collection.JavaConverters._

  private val partitions = new ConcurrentHashMap[String, PartitionMetadata]()

  override def getPartition(name: String): Option[PartitionMetadata] = Option(partitions.get(name))

  override def getPartitions(prefix: Option[String]): Seq[PartitionMetadata] = {
    val values = partitions.values().asScala
    prefix match {
      case None => values.toSeq
      case Some(p) => values.filter(_.name.startsWith(p)).toSeq
    }
  }

  override def addPartition(partition: PartitionMetadata): Unit = {
    val config = {
      val action = PartitionAction.Add
      val files = partition.files.toSet
      val envelope = EnvelopeConfig(partition.bounds.map(_.envelope).getOrElse(new Envelope()))
      PartitionConfig(partition.name, action, files, partition.count, envelope, System.currentTimeMillis())
    }
    FileBasedMetadata.writePartitionConfig(fc, directory, config)
    partitions.merge(partition.name, partition, FileBasedMetadata.add)
  }

  override def removePartition(partition: PartitionMetadata): Unit = {
    val config = {
      val action = PartitionAction.Remove
      val files = partition.files.toSet
      val envelope = EnvelopeConfig(partition.bounds.map(_.envelope).getOrElse(new Envelope()))
      PartitionConfig(partition.name, action, files, partition.count, envelope, System.currentTimeMillis())
    }
    FileBasedMetadata.writePartitionConfig(fc, directory, config)
    partitions.merge(partition.name, partition, FileBasedMetadata.remove)
  }

  override def compact(partition: Option[String], threads: Int): Unit = reload(partition, threads, write = true)

  override def reload(): Unit = reload(None, 4, write = false)

  override def close(): Unit = {}

  private def reload(partition: Option[String], threads: Int, write: Boolean): Unit = {
    require(threads > 0, "Threads must be a positive number")
    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(threads))
    try {
      // read the compaction baseline file
      val compacted = FileBasedMetadata.readCompactedConfig(fc, directory)
      // read any updates to the given partition(s)
      // use a parallel collection, so that we get some threading on reading and deleting individual update files
      val paths = FileBasedMetadata.listPartitionConfigs(ec, fc, directory, partition).par
      paths.tasksupport = new ExecutionContextTaskSupport(ec)
      val updates = paths.flatMap(FileBasedMetadata.readPartitionConfig(fc, _)).seq

      partition match {
        case None =>
          // group by partition and merge updates by timestamp
          val grouped = (compacted ++ updates).groupBy(_.name).values
          val merged = grouped.flatMap(mergePartitionConfigs).filter(_.files.nonEmpty).toList

          // update the in-memory map - update key by key, instead of clearing the map and repopulating it,
          // so that callers don't ever see an empty map
          val keys = new java.util.HashSet(partitions.keySet())
          merged.foreach { m =>
            keys.remove(m.name)
            partitions.put(m.name, m.toMetadata)
          }
          keys.asScala.foreach(partitions.remove)

          if (write) {
            FileBasedMetadata.writeCompactedConfig(fc, directory, merged)
            paths.foreach(fc.delete(_, false)) // note: already a parallel collection
          }

        case Some(p) =>
          // get the compacted baseline just for the partition we're reading
          val (matched, others) = compacted.partition(_.name == p)
          // merge the updates by timestamp
          val merged = mergePartitionConfigs(updates ++ matched).filter(_.files.nonEmpty)
          // only update the single partition entry in the in-memory map
          merged match {
            case None => partitions.remove(partition)
            case Some(m) => partitions.put(p, m.toMetadata)
          }

          if (write) {
            // we keep the baseline compaction for other partitions but update the one we're reading
            FileBasedMetadata.writeCompactedConfig(fc, directory, others ++ merged)
            paths.foreach(fc.delete(_, false)) // note: already a parallel collection
          }
      }
    } finally {
      ec.shutdown()
    }
  }
}

object FileBasedMetadata extends MethodProfiling with LazyLogging {

  import scala.collection.JavaConverters._

  val MetadataType = "file"
  val DefaultOptions = NamedOptions(MetadataType)

  private val CompactedPath    = "compacted.json"
  private val UpdateFilePrefix = "update-"
  private val JsonPathSuffix   = ".json"

  private val options = ConfigRenderOptions.concise().setFormatted(true)

  // function to add/merge an existing partition in an atomic call
  private val add = new java.util.function.BiFunction[PartitionMetadata, PartitionMetadata, PartitionMetadata]() {
    override def apply(existing: PartitionMetadata, update: PartitionMetadata): PartitionMetadata =
      existing + update
  }

  // function to remove/merge an existing partition in an atomic call
  private val remove = new java.util.function.BiFunction[PartitionMetadata, PartitionMetadata, PartitionMetadata]() {
    override def apply(existing: PartitionMetadata, update: PartitionMetadata): PartitionMetadata = {
      val result = existing - update
      if (result.files.isEmpty) { null } else { result }
    }
  }

  /**
    * Write metadata for a single partition operation to disk
    *
    * @param fc file context
    * @param directory metadata path
    * @param config partition config
    */
  private def writePartitionConfig(fc: FileContext, directory: Path, config: PartitionConfig): Unit = {
    val name = s"$UpdateFilePrefix${sanitizePartitionName(config.name)}-${UUID.randomUUID()}$JsonPathSuffix"
    val data = profile("Serialized partition configuration") {
      ConfigWriter[PartitionConfig].to(config).render(options)
    }
    profile("Persisted partition configuration") {
      val file = new Path(directory, name)
      WithClose(fc.create(file, java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent)) { out =>
        out.write(data.getBytes(StandardCharsets.UTF_8))
        out.hflush()
        out.hsync()
      }
      PathCache.register(fc, file)
    }
  }

  /**
    * Read and parse a partition metadata file
    *
    * @param fc file context
    * @param file file path
    * @return
    */
  private def readPartitionConfig(fc: FileContext, file: Path): Option[PartitionConfig] = {
    try {
      val config = profile("Loaded partition configuration") {
        WithClose(new InputStreamReader(fc.open(file), StandardCharsets.UTF_8)) { in =>
          ConfigFactory.parseReader(in, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
        }
      }
      profile("Parsed partition configuration") {
        Some(pureconfig.loadConfigOrThrow[PartitionConfig](config))
      }
    } catch {
      case NonFatal(e) => logger.error(s"Error reading config at path $file:", e); None
    }
  }

  /**
    * Write metadata for a single partition operation to disk
    *
    * @param fc file context
    * @param directory metadata path
    * @param config partition config
    */
  private def writeCompactedConfig(fc: FileContext, directory: Path, config: Seq[PartitionConfig]): Unit = {
    val data = profile("Serialized compacted partition configuration") {
      ConfigWriter[CompactedConfig].to(CompactedConfig(config)).render(options)
    }
    profile("Persisted compacted partition configuration") {
      val file = new Path(directory, CompactedPath)
      val flags = java.util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
      WithClose(fc.create(file, flags, CreateOpts.createParent)) { out =>
        out.write(data.getBytes(StandardCharsets.UTF_8))
        out.hflush()
        out.hsync()
      }
      PathCache.register(fc, file)
    }
  }

  /**
    * Read and parse a partition metadata file
    *
    * @param fc file context
    * @param directory metadata path
    * @return
    */
  private def readCompactedConfig(fc: FileContext, directory: Path): Seq[PartitionConfig] = {
    val file = new Path(directory, CompactedPath)
    try {
      if (!PathCache.exists(fc, file, reload = true)) { Seq.empty } else {
        val config = profile("Loaded compacted partition configuration") {
          WithClose(new InputStreamReader(fc.open(file), StandardCharsets.UTF_8)) { in =>
            ConfigFactory.parseReader(in, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
          }
        }
        profile("Parsed compacted partition configuration") {
          pureconfig.loadConfigOrThrow[CompactedConfig](config).partitions
        }
      }
    } catch {
      case NonFatal(e) => logger.error(s"Error reading config at path $file:", e); Seq.empty
    }
  }

  /**
    * Read any partition update paths from disk
    *
    * @param es executor service used for multi-threading
    * @param fc file context
    * @param directory metadata path
    * @param partition partition to read, or read all partitions
    * @return
    */
  private def listPartitionConfigs(
      es: ExecutorService,
      fc: FileContext,
      directory: Path,
      partition: Option[String]): Seq[Path] = {
    profile("Listed metadata files") {
      if (!PathCache.exists(fc, directory, reload = true)) { Seq.empty } else {
        val prefix = partition.map(p => s"$UpdateFilePrefix${sanitizePartitionName(p)}").getOrElse(UpdateFilePrefix)
        val result = new ConcurrentLinkedQueue[Path]()
        // use a phaser to track worker thread completion
        val phaser = new Phaser(2) // 1 for this thread + 1 for initial directory worker
        es.submit(new DirectoryWorker(es, phaser, fc, directory, result, prefix))
        // wait for the worker threads to complete
        phaser.awaitAdvanceInterruptibly(phaser.arrive())
        result.asScala.toSeq
      }
    }
  }

  private def sanitizePartitionName(name: String): String = name.replaceAll("[^a-zA-Z0-9]", "-")

  private class DirectoryWorker(
      es: ExecutorService,
      phaser: Phaser,
      fc: FileContext,
      dir: Path,
      result: ConcurrentLinkedQueue[Path],
      prefix: String
  ) extends Runnable {

    override def run(): Unit = {
      try {
        val iter = fc.listStatus(dir)
        while (iter.hasNext) {
          val status = iter.next
          val path = status.getPath
          if (status.isDirectory) {
            phaser.register() // register the new worker thread
            es.submit(new DirectoryWorker(es, phaser, fc, path, result, prefix))
          } else {
            val name = path.getName
            if (name.startsWith(prefix) && name.endsWith(JsonPathSuffix)) {
              result.add(path)
            }
          }
        }
      } finally {
        phaser.arrive() // notify that this thread is done
      }
    }
  }
}
