/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.io.InputStreamReader
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Envelope
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs._
import org.locationtech.geomesa.fs.storage.api.{PartitionMetadata, PartitionScheme}
import org.locationtech.geomesa.fs.storage.common.StorageMetadata.PartitionAction.PartitionAction
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.SimpleFeatureType
import pureconfig.{ConfigConvert, ConfigWriter}

import scala.util.control.NonFatal

/**
  * StorageMetadata implementation. Saves changes as a series of timestamped changelogs. This allows for
  * concurrent modification. The current state is obtained by replaying all the logs
  *
  * @param fc file context
  * @param root storage root path
  * @param sft simple feature type
  * @param scheme partition scheme
  * @param encoding file encoding
  */
class StorageMetadata private (fc: FileContext,
                               root: Path,
                               sft: SimpleFeatureType,
                               scheme: PartitionScheme,
                               encoding: String)
    extends org.locationtech.geomesa.fs.storage.api.StorageMetadata {

  import StorageMetadata._

  import scala.collection.JavaConverters._

  private val partitions = new ConcurrentHashMap[String, PartitionMetadata]()

  executor.scheduleWithFixedDelay(new ReloadRunnable(this), delay, delay, TimeUnit.MILLISECONDS)

  override def getRoot: Path = root

  override def getFileContext: FileContext = fc

  override def getSchema: SimpleFeatureType = sft

  override def getPartitionScheme: PartitionScheme = scheme

  override def getEncoding: String = encoding

  override def getPartition(name: String): PartitionMetadata = partitions.get(name)

  override def getPartitions: java.util.List[PartitionMetadata] = new java.util.ArrayList(partitions.values())

  override def addPartition(partition: PartitionMetadata): Unit = {
    val name = partition.name
    val bounds = partition.bounds()
    val config = PartitionConfig(name, PartitionAction.Add, partition.files().asScala.toSet, partition.count,
      EnvelopeConfig(bounds), System.currentTimeMillis())
    StorageMetadata.writePartitionConfig(fc, root, config)
    synchronized {
      val existing = partitions.remove(name)
      if (existing == null) {
        partitions.put(name, partition)
      } else {
        bounds.expandToInclude(existing.bounds())
        val files = new java.util.ArrayList[String](existing.files)
        files.addAll(partition.files)
        partitions.put(name, new PartitionMetadata(name, files, existing.count + partition.count, bounds))
      }
    }
  }

  override def removePartition(partition: PartitionMetadata): Unit = {
    val name = partition.name
    val bounds = partition.bounds()
    val config = PartitionConfig(name, PartitionAction.Remove, partition.files().asScala.toSet, partition.count,
      EnvelopeConfig(bounds), System.currentTimeMillis())
    StorageMetadata.writePartitionConfig(fc, root, config)
    synchronized {
      val existing = partitions.remove(name)
      if (existing != null) {
        val files = new java.util.ArrayList[String](existing.files)
        files.removeAll(partition.files)
        if (!files.isEmpty) {
          val update = new PartitionMetadata(name, files, math.max(0, existing.count + partition.count), bounds)
          partitions.put(name, update)
        }
      }
    }
  }

  override def compact(): Unit = {
    val (paths, merged) = loadMergedConfigs()
    paths.foreach(fc.delete(_, false))
    merged.foreach(StorageMetadata.writePartitionConfig(fc, root, _))
  }

  override def reload(): Unit = loadMergedConfigs()

  private def loadMergedConfigs(): (Seq[Path], Seq[PartitionConfig]) = {
    val paths = StorageMetadata.listPartitionConfigs(fc, root)
    val configs = paths.flatMap(StorageMetadata.readPartitionConfig(fc, _))
    val merged = configs.groupBy(_.name).toSeq.flatMap { case (_, updates) =>
      updates.sortBy(_.timestamp).dropWhile(_.action != PartitionAction.Add).reduceLeftOption { (result, update) =>
        update.action match {
          case PartitionAction.Add    => result.add(update)
          case PartitionAction.Remove => result.remove(update)
        }
      }
    }
    synchronized {
      partitions.clear()
      merged.foreach { m =>
        if (m.files.nonEmpty) {
          partitions.put(m.name, new PartitionMetadata(m.name, m.files.toSeq.asJava, m.count, m.envelope.toEnvelope))
        }
      }
    }
    (paths, merged)
  }
}

object StorageMetadata extends MethodProfiling with LazyLogging {

  import scala.collection.JavaConverters._

  private val MetadataDirectory = "metadata"

  private val MetadataPath = s"$MetadataDirectory/storage.json"

  private val UpdateFilePrefix = "update-"

  private val UpdatePathPrefix = s"$MetadataDirectory/$UpdateFilePrefix"

  private val JsonPathSuffix = ".json"

  private val options = ConfigRenderOptions.concise().setFormatted(true)

  private val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1))

  private val delay = PathCache.CacheDurationProperty.toDuration.get.toMillis

  private val cache = Caffeine.newBuilder().build(
    new CacheLoader[(FileContext, Path), StorageMetadata]() {
      // note: returning null will cause it to attempt to load again the next time it's accessed
      override def load(key: (FileContext, Path)): StorageMetadata = loadStorage(key._1, key._2).orNull
    }
  )

  /**
    * Creates a new, empty metadata and persists it
    *
    * @param fc file system
    * @param root root file system path
    * @param sft simple feature type
    * @param encoding fs encoding
    * @param scheme partition scheme
    * @return
    */
  def create(fc: FileContext,
             root: Path,
             sft: SimpleFeatureType,
             encoding: String,
             scheme: PartitionScheme): StorageMetadata = {
    val file = new Path(root, MetadataPath)
    // invalidate the path cache so we check the underlying fs, but then cache the result again
    PathCache.invalidate(fc, file)
    if (PathCache.exists(fc, file)) {
      throw new IllegalArgumentException(s"Metadata file already exists at path '$file'")
    }
    val sftConfig = SimpleFeatureTypes.toConfig(sft, includePrefix = false, includeUserData = true)
    writeStorageConfig(fc, root, StorageConfig(sftConfig, PartitionScheme.toConfig(scheme), encoding))
    val metadata = new StorageMetadata(fc, root, sft, scheme, encoding)
    cache.put((fc, root), metadata)
    metadata
  }

  /**
    * Loads a metadata instance from an existing file. Will return a cached instance, if available. If
    * a previous check was made to load a file from this root, and the file did not exist, will
    * not re-attempt to load it until after a configurable timeout.
    *
    * @see `org.locationtech.geomesa.fs.storage.common.utils.PathCache#CacheDurationProperty()`
    * @param fc file system
    * @param root path to the persisted metadata
    * @return
    */
  def load(fc: FileContext, root: Path): Option[StorageMetadata] = Option(cache.get((fc, root)))

  /**
    * Write metadata for a single partition operation to disk
    *
    * @param fc file context
    * @param root root path
    * @param config partition config
    */
  def writePartitionConfig(fc: FileContext, root: Path, config: PartitionConfig): Unit = {
    val name = s"$UpdatePathPrefix${config.name.replaceAll("[^a-zA-Z0-9]", "-")}-${UUID.randomUUID()}$JsonPathSuffix"
    val data = profile("Serialized partition configuration") {
      ConfigWriter[PartitionConfig].to(config).render(options)
    }
    profile("Persisted partition configuration") {
      val file = new Path(root, name)
      WithClose(fc.create(file, java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent)) { out =>
        out.writeBytes(data)
        out.hflush()
        out.hsync()
      }
      PathCache.invalidate(fc, file)
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
        WithClose(new InputStreamReader(fc.open(file))) { in =>
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
    * Read any partition operations from disk
    *
    * @param fc file context
    * @param root root path
    * @return
    */
  private def listPartitionConfigs(fc: FileContext, root: Path): Seq[Path] = {
    val directory = new Path(root, MetadataDirectory)
    if (!PathCache.exists(fc, directory)) { Seq.empty } else {
      val paths = Seq.newBuilder[Path]
      val iter = fc.util.listFiles(directory, true)
      while (iter.hasNext) {
        val path = iter.next.getPath
        val name = path.getName
        if (name.startsWith(UpdateFilePrefix) && name.endsWith(JsonPathSuffix)) {
          paths += path
        }
      }
      paths.result()
    }
  }

  /**
    * Attempts to load a stored metadata from the given path
    *
    * @param fc file context
    * @param root root path
    * @return
    */
  private def loadStorage(fc: FileContext, root: Path): Option[StorageMetadata] = {
    val metadata = readStorageConfig(fc, root).map { config =>
      val sft = profile("Created SimpleFeatureType") {
        SimpleFeatureTypes.createType(config.featureType, path = None)
      }
      // Load partition scheme - note we currently have to reload the SFT user data manually
      // which is why we have to add the partition scheme back to the SFT
      val scheme = profile("Created partition scheme") {
        PartitionScheme(sft, config.partitionScheme)
      }
      PartitionScheme.addToSft(sft, scheme)

      val storage = new StorageMetadata(fc, root, sft, scheme, config.encoding)
      storage.reload()
      storage
    }
    metadata.orElse(transitionMetadata(fc, root))
  }

  /**
    * Write a storage config to disk. This should be done once, when the storage is created
    *
    * @param fc file context
    * @param root root path
    * @param config config
    */
  private def writeStorageConfig(fc: FileContext, root: Path, config: StorageConfig): Unit = {
    val file = new Path(root, MetadataPath)
    val data = profile("Serialized storage configuration") {
      ConfigWriter[StorageConfig].to(config).render(options)
    }
    profile("Persisted storage configuration") {
      WithClose(fc.create(file, java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent)) { out =>
        out.writeBytes(data)
        out.hflush()
        out.hsync()
      }
    }
    PathCache.invalidate(fc, file)
  }

  /**
    * Read a storage config from disk
    *
    * @param fc file context
    * @param root root path
    * @return
    */
  private def readStorageConfig(fc: FileContext, root: Path): Option[StorageConfig] = {
    val file = new Path(root, MetadataPath)
    if (!PathCache.exists(fc, file)) { None } else {
      val config = profile("Loaded storage configuration") {
        WithClose(new InputStreamReader(fc.open(file))) { in =>
          ConfigFactory.parseReader(in, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
        }
      }
      profile("Parsed storage configuration") {
        Some(pureconfig.loadConfigOrThrow[StorageConfig](config))
      }
    }
  }

  // case classes for serializing to disk

  case class StorageConfig(featureType: Config, partitionScheme: Config, encoding: String)

  case class PartitionConfig(name: String, action: PartitionAction, files: Set[String], count: Long,
                             envelope: EnvelopeConfig, timestamp: Long) {
    def add(other: PartitionConfig): PartitionConfig = {
      require(action == PartitionAction.Add, "Can't aggregate into non-add actions")
      PartitionConfig(name, action, files ++ other.files, count + other.count, envelope.merge(other.envelope), timestamp)
    }

    def remove(other: PartitionConfig): PartitionConfig = {
      require(action == PartitionAction.Add, "Can't aggregate into non-add actions")
      PartitionConfig(name, action, files -- other.files, math.max(0L, count - other.count), envelope, timestamp)
    }
  }

  case class EnvelopeConfig(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {
    def merge(env: EnvelopeConfig): EnvelopeConfig = {
      // xmax < xmin indicates a null envelope
      if (xmax < xmin) {
        env
      } else if (env.xmax < env.xmin) {
        this
      } else {
        EnvelopeConfig(math.min(xmin, env.xmin), math.min(ymin, env.ymin),
          math.max(xmax, env.xmax), math.max(ymax, env.ymax))
      }
    }
    def toEnvelope: Envelope = new Envelope(xmin, xmax, ymin, ymax)
  }

  object EnvelopeConfig {
    def apply(env: Envelope): EnvelopeConfig = EnvelopeConfig(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    val empty: EnvelopeConfig = apply(new Envelope)
  }

  object PartitionAction extends Enumeration {
    type PartitionAction = Value
    val Add, Remove, Clear = Value
  }

  implicit val ActionConvert: ConfigConvert[PartitionAction] =
    ConfigConvert[String].xmap[PartitionAction](name => PartitionAction.values.find(_.toString == name).get, _.toString)

  private class ReloadRunnable(storage: StorageMetadata) extends Runnable {
    override def run(): Unit = storage.reload()
  }

  /**
    * Transition the old single-file metadata.json to the new append-log format
    *
    * @param fc file context
    * @param root root path
    * @return
    */
  private def transitionMetadata(fc: FileContext, root: Path): Option[StorageMetadata] = {
    val old = new Path(root, "metadata.json")
    if (!PathCache.exists(fc, old)) { None } else {
      val oldConfig = WithClose(new InputStreamReader(fc.open(old))) { in =>
        ConfigFactory.parseReader(in, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
      }

      val config = StorageConfig(oldConfig.getConfig("featureType"), oldConfig.getConfig("partitionScheme"),
        oldConfig.getString("encoding"))
      writeStorageConfig(fc, root, config)

      val sft = profile("Created SimpleFeatureType") {
        SimpleFeatureTypes.createType(config.featureType, path = None)
      }

      // Load partition scheme - note we currently have to reload the SFT user data manually
      // which is why we have to add the partition scheme back to the SFT
      val scheme = profile("Created partition scheme") {
        PartitionScheme(sft, config.partitionScheme)
      }
      PartitionScheme.addToSft(sft, scheme)

      val storage = new StorageMetadata(fc, root, sft, scheme, config.encoding)

      val partitionConfig = oldConfig.getConfig("partitions")
      partitionConfig.root().entrySet().asScala.foreach { e =>
        val name = e.getKey
        val files = partitionConfig.getStringList(name)
        writePartitionConfig(fc, root, PartitionConfig(name, PartitionAction.Add, files.asScala.toSet, 0L,
          EnvelopeConfig.empty, System.currentTimeMillis()))
        storage.partitions.put(name, new PartitionMetadata(name, files, 0L, new Envelope()))
      }

      fc.delete(old, false)
      PathCache.invalidate(fc, old)

      Some(storage)
    }
  }
}
