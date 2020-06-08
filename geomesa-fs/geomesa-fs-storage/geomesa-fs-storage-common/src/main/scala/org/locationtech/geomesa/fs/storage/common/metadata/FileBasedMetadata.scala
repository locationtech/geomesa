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
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionMetadata
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionScheme, StorageMetadata}
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.locationtech.geomesa.utils.text.StringSerialization
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.SimpleFeatureType

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
  * @param sft simple feature type
  * @param encoding file encoding
  * @param scheme partition scheme
  * @param leafStorage leaf storage
  */
class FileBasedMetadata(
    private val fc: FileContext,
    val directory: Path,
    val sft: SimpleFeatureType,
    val encoding: String,
    val scheme: PartitionScheme,
    val leafStorage: Boolean
  ) extends StorageMetadata with MethodProfiling with LazyLogging {

  import FileBasedMetadata._

  import scala.collection.JavaConverters._

  private val expiry = PathCache.CacheDurationProperty.toDuration.get.toMillis

  // cache of files associated with each partition
  // we use a cache to provide lazy non-blocking refresh, but the cache will only ever have 1 element in it
  private val partitions: LoadingCache[BoxedUnit, ConcurrentMap[String, PartitionFiles]] =
    Caffeine.newBuilder().refreshAfterWrite(expiry, TimeUnit.MILLISECONDS).build(
      new CacheLoader[BoxedUnit, ConcurrentMap[String, PartitionFiles]]() {
        override def load(key: BoxedUnit): ConcurrentMap[String, PartitionFiles] = readPartitionFiles(8)
      }
    )

  // cache of parsed metadata, keyed by partition
  private val metadata: LoadingCache[String, PartitionMetadata] =
    Caffeine.newBuilder().refreshAfterWrite(expiry, TimeUnit.MILLISECONDS).build(
      new CacheLoader[String, PartitionMetadata]() {
        override def load(key: String): PartitionMetadata =
          Option(partitions.get(BoxedUnit.UNIT).get(key)).flatMap(readPartition(_, 8)).map(_.toMetadata).orNull
      }
    )

  override def getPartitions(prefix: Option[String]): Seq[PartitionMetadata] = {
    partitions.get(BoxedUnit.UNIT).asScala.toStream.flatMap { case (p, _) =>
      if (prefix.forall(p.startsWith)) { Option(metadata.get(p)) } else { None }
    }
  }

  override def getPartition(name: String): Option[PartitionMetadata] = Option(metadata.get(name))

  override def addPartition(partition: PartitionMetadata): Unit = {
    val config = {
      val action = PartitionAction.Add
      val files = partition.files.toSet
      val envelope = EnvelopeConfig(partition.bounds.map(_.envelope).getOrElse(new Envelope()))
      PartitionConfig(partition.name, action, files, partition.count, envelope, System.currentTimeMillis())
    }
    val path = writePartition(config)
    // if we have already loaded the partition, merge in the new value
    if (metadata.getIfPresent(partition.name) != null) {
      metadata.asMap.merge(partition.name, partition, addMetadata)
    }
    Option(partitions.getIfPresent(BoxedUnit.UNIT)).foreach { files =>
      files.merge(partition.name, PartitionFiles(config = Seq(config), parsed = Seq(path)), addFiles)
    }
  }

  override def removePartition(partition: PartitionMetadata): Unit = {
    val config = {
      val action = PartitionAction.Remove
      val files = partition.files.toSet
      val envelope = EnvelopeConfig(partition.bounds.map(_.envelope).getOrElse(new Envelope()))
      PartitionConfig(partition.name, action, files, partition.count, envelope, System.currentTimeMillis())
    }
    val path = writePartition(config)
    // if we have already loaded the partition, merge in the new value
    if (metadata.getIfPresent(partition.name) != null) {
      metadata.asMap.merge(partition.name, partition, removeMetadata)
    }
    Option(partitions.getIfPresent(BoxedUnit.UNIT)).foreach { files =>
      files.merge(partition.name, PartitionFiles(config = Seq(config), parsed = Seq(path)), addFiles)
    }
  }

  override def compact(partition: Option[String], threads: Int): Unit = {
    require(threads > 0, "Threads must be a positive number")

    // in normal usage, we never pass in a partition to this method
    partition.foreach(p => logger.warn(s"Ignoring requested partition '$p' and compacting all metadata"))

    val configs = ArrayBuffer.empty[PartitionConfig]
    val paths = ArrayBuffer.empty[Path]

    readPartitionFiles(threads).asScala.foreach { case (name, f) =>
      val config = readPartition(f, threads).filter(_.files.nonEmpty)
      metadata.put(name, config.map(_.toMetadata).orNull)
      config.foreach(c => configs += c)
      paths ++= f.unparsed
      paths ++= f.parsed
    }

    writeCompactedConfig(configs)

    if (threads < 2) {
      paths.foreach(fc.delete(_, false))
    } else {
      val ec = ExecutionContext.fromExecutorService(new CachedThreadPool(threads))
      try {
        val parPaths = paths.par
        parPaths.tasksupport = new ExecutionContextTaskSupport(ec)
        parPaths.foreach(fc.delete(_, false))
      } finally {
        ec.shutdown()
      }
    }

    partitions.invalidate(BoxedUnit.UNIT)
  }

  override def close(): Unit = {}

  /**
   * Serialize a partition config to disk
   *
   * @param config config
   * @return
   */
  private def writePartition(config: PartitionConfig): Path = {
    val data = profile("Serialized partition configuration") {
      PartitionConfigConvert.to(config).render(options)
    }
    profile("Persisted partition configuration") {
      val encoded = StringSerialization.alphaNumericSafeString(config.name)
      val name = s"$UpdatePartitionPrefix$encoded-${UUID.randomUUID()}$JsonPathSuffix"
      val file = new Path(directory, name)
      WithClose(fc.create(file, java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent)) { out =>
        out.write(data.getBytes(StandardCharsets.UTF_8))
        out.hflush()
        out.hsync()
      }
      PathCache.register(fc, file)
      file
    }
  }

  /**
   * Write metadata for a compacted set of partition operations to disk
   *
   * @param config partition config
   */
  private def writeCompactedConfig(config: Seq[PartitionConfig]): Unit = {
    val data = profile("Serialized compacted partition configuration") {
      CompactedConfigConvert.to(CompactedConfig(config)).render(options)
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
   * Reads all the metadata files and groups them by partition, parsing them only if needed
   * to determine the partition name
   *
   * @param threads threads
   * @return
   */
  private def readPartitionFiles(threads: Int): ConcurrentMap[String, PartitionFiles] = {
    val result = new ConcurrentHashMap[String, PartitionFiles]()

    // list all the metadata files on disk
    profile("Listed metadata files") {
      val pool = new CachedThreadPool(threads)
      // use a phaser to track worker thread completion
      val phaser = new Phaser(2) // 1 for the initial directory worker + 1 for this thread
      pool.submit(new DirectoryWorker(pool, phaser, directory, result))
      // wait for the worker threads to complete
      try {
        phaser.awaitAdvanceInterruptibly(phaser.arrive())
      } finally {
        pool.shutdown()
      }
    }

    result
  }

  /**
   * Parses and merges the config files for a given partition
   *
   * @param files files associated with the partition
   * @param threads threads
   * @return
   */
  private def readPartition(files: PartitionFiles, threads: Int): Option[PartitionConfig] = {
    val updates = if (threads < 2) {
      files.unparsed.flatMap(readPartitionConfig)
    } else {
      val ec = ExecutionContext.fromExecutorService(new CachedThreadPool(threads))
      try {
        val unparsed = files.unparsed.par
        unparsed.tasksupport = new ExecutionContextTaskSupport(ec)
        unparsed.flatMap(readPartitionConfig).seq
      } finally {
        ec.shutdown()
      }
    }
    mergePartitionConfigs(updates ++ files.config).filter(_.files.nonEmpty)
  }

  /**
   * Read and parse a partition metadata file
   *
   * @param file file path
   * @return
   */
  private def readPartitionConfig(file: Path): Option[PartitionConfig] = {
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
   * Read and parse a compacted partition metadata file
   *
   * @param file compacted config file
   * @return
   */
  private def readCompactedConfig(file: Path): Seq[PartitionConfig] = {
    try {
      val config = profile("Loaded compacted partition configuration") {
        WithClose(new InputStreamReader(fc.open(file), StandardCharsets.UTF_8)) { in =>
          ConfigFactory.parseReader(in, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
        }
      }
      profile("Parsed compacted partition configuration") {
        pureconfig.loadConfigOrThrow[CompactedConfig](config).partitions
      }
    } catch {
      case NonFatal(e) => logger.error(s"Error reading config at path $file:", e); Seq.empty
    }
  }

  private class DirectoryWorker(
      es: ExecutorService,
      phaser: Phaser,
      dir: Path,
      result: ConcurrentHashMap[String, PartitionFiles]
    ) extends Runnable {

    override def run(): Unit = {
      try {
        val iter = fc.listStatus(dir)
        while (iter.hasNext) {
          val status = iter.next
          val path = status.getPath
          lazy val name = path.getName
          if (status.isDirectory) {
            phaser.register() // register the new worker thread
            es.submit(new DirectoryWorker(es, phaser, path, result))
          } else if (name.startsWith(UpdatePartitionPrefix) && name.endsWith(JsonPathSuffix)) {
            // pull out the partition name but don't parse the file yet
            val encoded = name.substring(8, name.length - 42) // strip out prefix and suffix
            val partition = StringSerialization.decodeAlphaNumericSafeString(encoded)
            result.merge(partition, PartitionFiles(unparsed = Seq(path)), addFiles)
          } else if (name == CompactedPath) {
            phaser.register() // register the new worker thread
            es.submit(new CompactedParser(phaser, path, result))
          } else if (name.startsWith(UpdateFilePrefix) && name.endsWith(JsonPathSuffix)) {
            // old update files - have to parse them to get the partition name
            phaser.register() // register the new worker thread
            es.submit(new UpdateParser(phaser, path, result))
          }
        }
      } catch {
        case _: FileNotFoundException => // the partition dir was deleted... just return
        case NonFatal(e) => logger.error("Error scanning metadata directory:", e)
      } finally {
        phaser.arrive() // notify that this thread is done
      }
    }
  }

  private class CompactedParser(phaser: Phaser, path: Path, result: ConcurrentHashMap[String, PartitionFiles])
      extends Runnable {

    override def run(): Unit = {
      try {
        readCompactedConfig(path).foreach { config =>
          // note: don't track the path since it's from the compacted config file
          result.merge(config.name, PartitionFiles(config = Seq(config)), addFiles)
        }
      } catch {
        case _: FileNotFoundException => // the file was deleted... just return
        case NonFatal(e) => logger.error("Error reading compacted metadata entry:", e)
      } finally {
        phaser.arrive() // notify that this thread is done
      }
    }
  }

  private class UpdateParser(phaser: Phaser, path: Path, result: ConcurrentHashMap[String, PartitionFiles])
      extends Runnable {

    override def run(): Unit = {
      try {
        readPartitionConfig(path).foreach { config =>
          result.merge(config.name, PartitionFiles(config = Seq(config), parsed = Seq(path)), addFiles)
        }
      } catch {
        case _: FileNotFoundException => // the file was deleted... just return
        case NonFatal(e) => logger.error("Error reading metadata update entry:", e)
      } finally {
        phaser.arrive() // notify that this thread is done
      }
    }
  }
}

object FileBasedMetadata {

  val MetadataType = "file"
  val DefaultOptions: NamedOptions = NamedOptions(MetadataType)

  private val CompactedPath         = "compacted.json"
  private val UpdateFilePrefix      = "update-"
  private val UpdatePartitionPrefix = UpdateFilePrefix + "$"
  private val JsonPathSuffix        = ".json"

  private val options = ConfigRenderOptions.concise().setFormatted(true)

  // function to add/merge an existing partition in an atomic call
  private val addMetadata = new BiFunction[PartitionMetadata, PartitionMetadata, PartitionMetadata]() {
    override def apply(existing: PartitionMetadata, update: PartitionMetadata): PartitionMetadata =
      existing + update
  }

  // function to remove/merge an existing partition in an atomic call
  private val removeMetadata = new BiFunction[PartitionMetadata, PartitionMetadata, PartitionMetadata]() {
    override def apply(existing: PartitionMetadata, update: PartitionMetadata): PartitionMetadata = {
      val result = existing - update
      if (result.files.isEmpty) { null } else { result }
    }
  }

  // function to merge partition files in an atomic call
  private val addFiles = new BiFunction[PartitionFiles, PartitionFiles, PartitionFiles]() {
    override def apply(existing: PartitionFiles, update: PartitionFiles): PartitionFiles = {
      val config = existing.config ++ update.config
      PartitionFiles(config, existing.parsed ++ update.parsed, existing.unparsed ++ update.unparsed)
    }
  }

  /**
   * Copy a metadata instance. Discards any cached state
   *
   * @param m metadata
   * @return
   */
  def copy(m: FileBasedMetadata): FileBasedMetadata =
    new FileBasedMetadata(m.fc, m.directory, m.sft, m.encoding, m.scheme, m.leafStorage)

  /**
   * Holder for metadata files for a partition
   *
   * @param config any parsed configurations
   * @param parsed references to the files corresponding to `config`
   * @param unparsed unparsed config files associated with the partition
   */
  private case class PartitionFiles(
      config: Seq[PartitionConfig] = Seq.empty,
      parsed: Seq[Path] = Seq.empty,
      unparsed: Seq[Path] = Seq.empty
    )
}
