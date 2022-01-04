/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.concurrent.{CachedThreadPool, PhaserUtils}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.locationtech.geomesa.utils.text.StringSerialization
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
 * @param meta basic metadata config
 * @param converter file converter
 */
class FileBasedMetadata(
    private val fc: FileContext,
    val directory: Path,
    val sft: SimpleFeatureType,
    private val meta: Metadata,
    private val converter: MetadataConverter
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

  override val scheme: PartitionScheme = PartitionSchemeFactory.load(sft, meta.scheme)
  override val encoding: String = meta.config(Metadata.Encoding)
  override val leafStorage: Boolean = meta.config(Metadata.LeafStorage).toBoolean

  private val kvs = new ConcurrentHashMap[String, String](meta.config.asJava)

  override def get(key: String): Option[String] = Option(kvs.get(key))

  override def set(key: String, value: String): Unit = {
    kvs.put(key, value)
    FileBasedMetadataFactory.write(fc, directory.getParent, meta.copy(config = kvs.asScala.toMap))
  }

  override def getPartitions(prefix: Option[String]): Seq[PartitionMetadata] = {
    partitions.get(BoxedUnit.UNIT).asScala.toStream.flatMap { case (p, _) =>
      if (prefix.forall(p.startsWith)) { Option(metadata.get(p)) } else { None }
    }
  }

  override def getPartition(name: String): Option[PartitionMetadata] = Option(metadata.get(name))

  override def addPartition(partition: PartitionMetadata): Unit = {
    val config = {
      val action = PartitionAction.Add
      val envelope = partition.bounds.map(b => EnvelopeConfig(b.envelope)).getOrElse(Seq.empty)
      PartitionConfig(partition.name, action, partition.files, partition.count, envelope, System.currentTimeMillis())
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
      val envelope = partition.bounds.map(b => EnvelopeConfig(b.envelope)).getOrElse(Seq.empty)
      PartitionConfig(partition.name, action, partition.files, partition.count, envelope, System.currentTimeMillis())
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

  override def setPartitions(partitions: Seq[PartitionMetadata]): Unit = {
    val map = new ConcurrentHashMap[String, PartitionFiles]
    this.partitions.put(BoxedUnit.UNIT, map)
    this.metadata.invalidateAll()

    val configs = partitions.map { partition =>
      val action = PartitionAction.Add
      val envelope = partition.bounds.map(b => EnvelopeConfig(b.envelope)).getOrElse(Seq.empty)
      val config = PartitionConfig(partition.name, action, partition.files, partition.count, envelope, System.currentTimeMillis())
      // note: side effects in map
      this.metadata.put(partition.name, partition)
      map.put(partition.name, PartitionFiles(config = Seq(config)))
      config
    }

    writeCompactedConfig(configs)
    delete(readPartitionFiles(8).asScala.flatMap { case (_, f) => f.unparsed ++ f.parsed }, 8)
  }

  // noinspection ScalaDeprecation
  override def compact(partition: Option[String], threads: Int): Unit = compact(partition, None, threads)

  override def compact(partition: Option[String], fileSize: Option[Long], threads: Int): Unit = {
    require(threads > 0, "Threads must be a positive number")

    // in normal usage, we never pass in a partition to this method
    partition.foreach(p => logger.warn(s"Ignoring requested partition '$p' and compacting all metadata"))

    val configs = ArrayBuffer.empty[PartitionConfig]
    val paths = ArrayBuffer.empty[Path]

    readPartitionFiles(threads).asScala.foreach { case (name, f) =>
      val config = readPartition(f, threads).filter(_.files.nonEmpty)
      config match {
        case None => metadata.invalidate(name)
        case Some(c) =>
          metadata.put(name, c.toMetadata)
          configs += c
      }
      paths ++= f.unparsed
      paths ++= f.parsed
    }

    writeCompactedConfig(configs)
    delete(paths, threads)

    partitions.invalidate(BoxedUnit.UNIT)
  }

  override def invalidate(): Unit = {
    partitions.invalidateAll()
    metadata.invalidateAll()
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
      converter.renderPartition(config)
    }
    profile("Persisted partition configuration") {
      val encoded = StringSerialization.alphaNumericSafeString(config.name)
      val name = s"$UpdatePartitionPrefix$encoded-${UUID.randomUUID()}${converter.suffix}"
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
      converter.renderCompaction(config)
    }
    profile("Persisted compacted partition configuration") {
      val file = new Path(directory, CompactedPrefix + converter.suffix)
      val flags = java.util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
      WithClose(fc.create(file, flags, CreateOpts.createParent)) { out =>
        out.write(data.getBytes(StandardCharsets.UTF_8))
        out.hflush()
        out.hsync()
      }
      PathCache.register(fc, file)
      // generally we overwrite the existing file but if we change rendering the name will change
      val toRemove =
        new Path(directory, if (converter.suffix == HoconPathSuffix) { CompactedJson } else { CompactedHocon })
      if (PathCache.exists(fc, toRemove, reload = true)) {
        fc.delete(toRemove, false)
        PathCache.invalidate(fc, toRemove)
      }
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
      pool.submit(new DirectoryWorker(pool, phaser, fc.listStatus(directory), result))
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
          ConfigFactory.parseReader(in, ConfigParseOptions.defaults().setSyntax(getSyntax(file.getName)))
        }
      }
      profile("Parsed partition configuration") {
        Some(converter.parsePartition(config))
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
          ConfigFactory.parseReader(in, ConfigParseOptions.defaults().setSyntax(getSyntax(file.getName)))
        }
      }
      profile("Parsed compacted partition configuration") {
        converter.parseCompaction(config)
      }
    } catch {
      case NonFatal(e) => logger.error(s"Error reading config at path $file:", e); Seq.empty
    }
  }

  /**
   * Delete a seq of paths
   *
   * @param paths paths to delete
   * @param threads number of threads to use
   */
  private def delete(paths: Iterable[Path], threads: Int): Unit = {
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
  }

  private class DirectoryWorker(
      es: ExecutorService,
      phaser: Phaser,
      listDirectory: => RemoteIterator[FileStatus],
      result: ConcurrentHashMap[String, PartitionFiles]
    ) extends Runnable {

    override def run(): Unit = {
      try {
        var i = phaser.getRegisteredParties + 1
        val iter = listDirectory
        while (iter.hasNext && i < PhaserUtils.MaxParties) {
          val status = iter.next
          val path = status.getPath
          lazy val name = path.getName
          if (status.isDirectory) {
            i += 1
            // use a tiered phaser on each directory avoid the limit of 65535 registered parties
            es.submit(new DirectoryWorker(es, new Phaser(phaser, 1), fc.listStatus(path), result))
          } else if (name.startsWith(UpdatePartitionPrefix)) {
            // pull out the partition name but don't parse the file yet
            val encoded = name.substring(8, name.length - 42) // strip out prefix and suffix
            val partition = StringSerialization.decodeAlphaNumericSafeString(encoded)
            result.merge(partition, PartitionFiles(unparsed = Seq(path)), addFiles)
          } else if (name == CompactedHocon || name == CompactedJson) {
            i += 1
            phaser.register() // register the new worker thread
            es.submit(new CompactedParser(phaser, path, result))
          } else if (name.startsWith(UpdateFilePrefix) && name.endsWith(JsonPathSuffix)) {
            // old update files - have to parse them to get the partition name
            i += 1
            phaser.register() // register the new worker thread
            es.submit(new UpdateParser(phaser, path, result))
          }
        }
        if (iter.hasNext) {
          es.submit(new DirectoryWorker(es, new Phaser(phaser, 1), iter, result))
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
  val DefaultOptions: NamedOptions = NamedOptions(MetadataType, Map(Config.RenderKey -> Config.RenderCompact))
  val LegacyOptions : NamedOptions = NamedOptions(MetadataType, Map(Config.RenderKey -> Config.RenderPretty))

  object Config {
    val RenderKey = "render"

    val RenderPretty  = "pretty"
    val RenderCompact = "compact"
  }

  private val CompactedPrefix       = "compacted"
  private val UpdateFilePrefix      = "update-"
  private val UpdatePartitionPrefix = UpdateFilePrefix + "$"
  private val JsonPathSuffix        = RenderPretty.suffix
  private val HoconPathSuffix       = RenderCompact.suffix
  private val CompactedJson         = CompactedPrefix + JsonPathSuffix
  private val CompactedHocon        = CompactedPrefix + HoconPathSuffix

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
    new FileBasedMetadata(m.fc, m.directory, m.sft, m.meta, m.converter)

  private def getSyntax(file: String): ConfigSyntax = {
    if (file.endsWith(HoconPathSuffix)) {
      ConfigSyntax.CONF
    } else if (file.endsWith(JsonPathSuffix)) {
      ConfigSyntax.JSON
    } else {
      ConfigSyntax.JSON
    }
  }

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
