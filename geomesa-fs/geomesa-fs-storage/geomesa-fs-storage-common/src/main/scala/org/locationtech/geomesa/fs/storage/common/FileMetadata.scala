/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.io.{IOException, InputStreamReader}
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Envelope
import org.apache.hadoop.fs.Options.{CreateOpts, Rename}
import org.apache.hadoop.fs._
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.fs.storage.api.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * FileMetadata implementation. Persists to disk after any change - prefer bulk operations when possible.
  * Creates a backup file when it writes to disk and keeps the 5 most recent ones.
  *
  * @param fc file context
  * @param root storage root path
  * @param sft simple feature type
  * @param scheme partition scheme
  * @param encoding file encoding
  */
class FileMetadata private (fc: FileContext,
                            root: Path,
                            sft: SimpleFeatureType,
                            scheme: PartitionScheme,
                            encoding: String,
                            data: Option[Config])
    extends org.locationtech.geomesa.fs.storage.api.FileMetadata {

  import scala.collection.JavaConverters._

  private val partitions = new ConcurrentHashMap[String, java.util.Set[String]]()

  override def getRoot: Path = root

  override def getFileContext: FileContext = fc

  override def getSchema: SimpleFeatureType = sft

  override def getPartitionScheme: PartitionScheme = scheme

  override def getEncoding: String = encoding

  override def getPartitionCount: Int = partitions.size

  override def getFileCount: Int = {
    var count = 0
    partitions.asScala.foreach { case (_, files) => count += files.size() }
    count
  }

  override def getPartitions: java.util.List[String] = new java.util.ArrayList(partitions.keySet())

  override def getFiles(partition: String): java.util.List[String] =
    new java.util.ArrayList[String](partitions.get(partition))

  override def getPartitionFiles: java.util.Map[String, java.util.List[String]] = {
    val map = new java.util.HashMap[String, java.util.List[String]](partitions.size)
    partitions.asScala.foreach { case (k, v) => map.put(k, new java.util.ArrayList(v)) }
    map
  }

  override def setFiles(partitionsToFiles: java.util.Map[String, java.util.List[String]]): Unit = {
    val changed = partitions.size() != partitionsToFiles.size() ||
        partitionsToFiles.asScala.exists { case (partition, files) =>
          !Option(partitions.get(partition)).contains(new java.util.HashSet(files))
        }
    if (changed) {
      partitions.clear()
      partitionsToFiles.asScala.foreach { case (k, v) =>
        partitions.computeIfAbsent(k, FileMetadata.createSet).addAll(v)
      }
      FileMetadata.save(this)
    }
  }

  override def addFile(partition: String, file: String): Unit = {
    if (partitions.computeIfAbsent(partition, FileMetadata.createSet).add(file)) {
      FileMetadata.save(this)
    }
  }

  override def addFiles(partition: String, files: java.util.List[String]): Unit = {
    if (partitions.computeIfAbsent(partition, FileMetadata.createSet).addAll(files)) {
      FileMetadata.save(this)
    }
  }

  override def addFiles(partitionsToFiles: java.util.Map[String, java.util.List[String]]): Unit = {
    var changed = false
    partitionsToFiles.asScala.foreach { case (k, v) =>
      changed = partitions.computeIfAbsent(k, FileMetadata.createSet).addAll(v) || changed
    }
    if (changed) {
      FileMetadata.save(this)
    }
  }

  override def removeFile(partition: String, file: String): Unit = {
    if (Option(partitions.get(partition)).exists(_.remove(file))) {
      FileMetadata.save(this)
    }
  }

  override def removeFiles(partition: String, files: java.util.List[String]): Unit = {
    if (Option(partitions.get(partition)).exists(_.removeAll(files))) {
      FileMetadata.save(this)
    }
  }

  override def removeFiles(partitionsToFiles: java.util.Map[String, java.util.List[String]]): Unit = {
    var changed = false
    partitionsToFiles.asScala.foreach { case (k, v) =>
      changed = Option(partitions.get(k)).exists(_.removeAll(v)) || changed
    }
    if (changed) {
      FileMetadata.save(this)
    }
  }

  override def replaceFiles(partition: String, files: java.util.List[String], replacement: String): Unit = {
    val removed = Option(partitions.get(partition)).exists(_.removeAll(files))
    val added = partitions.computeIfAbsent(partition, FileMetadata.createSet).add(replacement)
    if (removed || added) {
      FileMetadata.save(this)
    }
  }

  var internalCount = data.map(_.getInt("count")).getOrElse(0)
  var bounds: Envelope = data.map { config =>
    if (config.hasPath("bounds")) {
    val doubles = config.getDoubleList("bounds")
      new ReferencedEnvelope(doubles(0), doubles(1), doubles(2), doubles(3), CRS_EPSG_4326)
    } else {
      null
    }
  }.orNull

  override def getFeatureCount: Int = internalCount
  override def increaseFeatureCount(count: Int): Unit = {
    internalCount += count
  }

  override def getEnvelope: ReferencedEnvelope =
    if (bounds == null) {
      ReferencedEnvelope.EVERYTHING
    } else {
      new ReferencedEnvelope(bounds, CRS_EPSG_4326)
    }
  override def expandBounds(envelope: Envelope): Unit = {
    if (bounds == null) {
      bounds = envelope
    } else {
      bounds = {
        bounds.expandToInclude(envelope)
        bounds
      }
    }
  }

  /**
    * Call to write updated metadata to disk.
    */
  override def persist(): Unit = {
    FileMetadata.save(this)
  }
}

object FileMetadata extends MethodProfiling with LazyLogging {

  val MetadataFileName = "metadata.json"

  private val createSet = new java.util.function.Function[String, java.util.Set[String]] {
    override def apply(t: String): java.util.Set[String] =
      Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean])
  }

  private val options = ConfigRenderOptions.concise().setFormatted(true)

  private val cache = Caffeine.newBuilder().build(
    new CacheLoader[(FileContext, Path), FileMetadata]() {
      // note: returning null will cause it to attempt to load again the next time it's accessed
      override def load(key: (FileContext, Path)): FileMetadata = loadFile(key._1, key._2).orNull
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
             scheme: PartitionScheme): FileMetadata = {
    val file = filePath(root)
    // invalidate the path cache so we check the underlying fs, but then cache the result again
    PathCache.invalidate(fc, file)
    if (PathCache.exists(fc, file)) {
      throw new IllegalArgumentException(s"Metadata file already exists at path '$file'")
    }
    val metadata = new FileMetadata(fc, root, sft, scheme, encoding, None)
    cache.put((fc, file), metadata)
    save(metadata)
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
  def load(fc: FileContext, root: Path): Option[FileMetadata] = Option(cache.get((fc, filePath(root))))

  /**
    * Path for the metadata file under a given root
    *
    * @param root storage root
    * @return
    */
  private def filePath(root: Path): Path = new Path(root, FileMetadata.MetadataFileName)

  /**
    * Attempts to load a stored metadata from the given path
    *
    * @param fc file context
    * @param file file (may or may not exist)
    * @return
    */
  private def loadFile(fc: FileContext, file: Path): Option[FileMetadata] = {
    if (!PathCache.exists(fc, file)) { None } else {
      val config = profile {
        WithClose(new InputStreamReader(fc.open(file))) { in =>
          ConfigFactory.parseReader(in, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
        }
      } ((_, time) => logger.trace(s"Loaded configuration in ${time}ms"))

      val sft = profile {
        SimpleFeatureTypes.createType(config.getConfig("featureType"), path = None)
      } ((_, time) => logger.debug(s"Created SimpleFeatureType in ${time}ms"))

      // Load encoding
      val encoding = config.getString("encoding")

      val data: Config = config.getConfig("data")

      // Load partition scheme - note we currently have to reload the SFT user data manually
      // which is why we have to add the partition scheme back to the SFT
      val scheme = PartitionScheme(sft, config.getConfig("partitionScheme"))
      PartitionScheme.addToSft(sft, scheme)

      val metadata = new FileMetadata(fc, file.getParent, sft, scheme, encoding, Some(data))

      // Load Partitions
      profile {
        import scala.collection.JavaConverters._
        val partitionConfig = config.getConfig("partitions")
        partitionConfig.root().entrySet().asScala.foreach { e =>
          val key = e.getKey
          val set = createSet.apply(null)
          set.addAll(partitionConfig.getStringList(key))
          metadata.partitions.put(key, set)
        }
      } ((_, time) => logger.debug(s"Loaded partitions in ${time}ms"))

      Some(metadata)
    }
  }

  /**
    * Overwrites the persisted file on disk, and creates a backup of the new file
    *
    * @param metadata metadata to persist
    */
  private def save(metadata: FileMetadata): Unit = metadata.synchronized {
    val fc = metadata.getFileContext
    val file = filePath(metadata.getRoot)

    val config = profile {
      var dataConfig: Config = ConfigFactory.empty()
        .withValue("count", ConfigValueFactory.fromAnyRef(metadata.getFeatureCount))

      if (metadata.getEnvelope != ReferencedEnvelope.EVERYTHING) {
        dataConfig = dataConfig.withValue("bounds", ConfigValueFactory.fromIterable(
          Seq[Double](metadata.getEnvelope.getMinX, metadata.getEnvelope.getMaxX,
            metadata.getEnvelope.getMinY, metadata.getEnvelope.getMaxY).asJava))
      }

      val sft = metadata.getSchema
      val sftConfig = SimpleFeatureTypes.toConfig(sft, includePrefix = false, includeUserData = true).root()
      ConfigFactory.empty()
        .withValue("featureType", sftConfig)
        .withValue("encoding", ConfigValueFactory.fromAnyRef(metadata.getEncoding))
        .withValue("partitionScheme", PartitionScheme.toConfig(metadata.getPartitionScheme).root())
        .withValue("partitions", ConfigValueFactory.fromMap(metadata.getPartitionFiles))
        .withValue("data", dataConfig.root())
        .root
        .render(options)
    } ((_, time) => logger.debug(s"Created config for persistence in ${time}ms"))

    Thread.sleep(1) // ensure that we have a unique nano time - nano time is only guaranteed to milli precision

    // write to a temporary file
    val tmp = file.suffix(s".tmp.${System.currentTimeMillis()}.${System.nanoTime()}")
    profile {
      WithClose(fc.create(tmp, java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent)) { out =>
        out.writeBytes(config)
        out.hflush()
        out.hsync()
      }
    } ((_, time) => logger.debug(s"Wrote temp file in ${time}ms"))

    // rename the file to the actual file name
    // note: this *should* be atomic, according to
    // http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/introduction.html#Atomicity
    profile {
      fc.rename(tmp, file, Rename.OVERWRITE)

      // Because of eventual consistency let's make sure it finished
      var tryNum = 0
      var done = false

      while (!done) {
        if (!fc.util.exists(tmp) && fc.util.exists(file)) {
          done = true
        } else if (tryNum < 4) {
          Thread.sleep((2 ^ tryNum) * 1000)
          tryNum += 1
        } else {
          throw new IOException(s"Unable to properly update metadata after $tryNum tries")
        }
      }
    } ((_, time) => logger.debug(s"Renamed metadata file in ${time}ms"))

    // back up the file and delete any extra backups beyond 5
    profile {
      val backup = file.suffix(s".old.${System.currentTimeMillis()}.${System.nanoTime()}")
      WithClose(fc.create(backup, java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent)) { out =>
        out.writeBytes(config)
        out.hflush()
        out.hsync()
      }
    } ((_, time) => logger.debug(s"Wrote backup metadata file in ${time}ms"))

    profile {
      val backups = Option(fc.util.globStatus(file.suffix(".old.*"))).getOrElse(Array.empty).map(_.getPath)

      // Keep the 5 most recent metadata files and delete the old ones
      backups.sortBy(_.getName)(Ordering.String.reverse).drop(5).foreach { backup =>
        logger.debug(s"Removing old metadata backup $backup")
        fc.delete(backup, false)
      }

      backups
    } ((files, time) => logger.debug(s"Deleted ${math.max(0, files.length - 5)} old backup files in ${time}ms"))
  }
}
