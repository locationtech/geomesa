/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import com.google.gson.reflect.TypeToken
import com.google.gson.{Gson, GsonBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs._
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFile
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.metadata.filter.CachedMetadata
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.StringSerialization

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.runtime.BoxedUnit
import scala.util.control.NonFatal

/**
 * StorageMetadata implementation that stores all file information in a single JSON file.
 * Uses HDFS lock files for cross-JVM write locking and synchronization for intra-JVM locking.
 *
 * @param fs file system
 * @param directory metadata root path
 * @param meta basic metadata config
 */
class FileBasedMetadata(
    private val fs: FileSystem,
    val directory: Path,
    private val meta: Metadata,
  ) extends StorageMetadata with CachedMetadata with LazyLogging {

  import FileBasedMetadata._

  import scala.collection.JavaConverters._

  private val kvs = new ConcurrentHashMap[String, String](meta.config.asJava)
  private val encodedTypeName = StringSerialization.alphaNumericSafeString(meta.sft.getTypeName)
  private val metadataFilePath = new Path(directory, s"$encodedTypeName.json")
  private val filesFilePath = new Path(directory, s".${encodedTypeName}_files.json")
  private val lockFilePath = new Path(directory, s".${encodedTypeName}_files.json.lock")

  override val `type`: String = FileBasedMetadata.MetadataType

  override val sft: SimpleFeatureType = meta.sft
  override val schemes: Set[PartitionScheme] = meta.partitions.map(PartitionSchemeFactory.load(sft, _)).toSet

  override def get(key: String): Option[String] = Option(kvs.get(key))

  // note: this isn't synchronized across jvms
  override def set(key: String, value: String): Unit = FileBasedMetadata.synchronized {
    kvs.put(key, value)
    WithClose(fs.create(metadataFilePath, true)) { out =>
      MetadataSerialization.serialize(out, meta.copy(config = kvs.asScala.toMap))
      out.hflush()
      out.hsync()
    }
  }

  override def addFile(file: StorageFile): Unit = {
    modifyFiles { files =>
      // remove any existing file with the same path and add the new one
      (files.filterNot(_.file == file.file) :+ file).sortBy(_.timestamp)(Ordering.Long.reverse)
    }
  }

  override def removeFile(file: StorageFile): Unit = {
    modifyFiles { files =>
      files.filterNot(_.file == file.file)
    }
  }

  override def replaceFiles(existing: Seq[StorageFile], replacements: Seq[StorageFile]): Unit = {
    val existingFiles = existing.map(_.file)
    modifyFiles { files =>
      files.filterNot(f => existingFiles.contains(f.file)) ++ replacements
    }
  }

  /**
   * Load files from the JSON file
   */
  override protected def buildFileList(): Seq[StorageFile] = {
    try {
      if (PathCache.exists(fs, filesFilePath) || PathCache.exists(fs, filesFilePath, reload = true)) {
        WithClose(fs.open(filesFilePath)) { in =>
          val listType = new TypeToken[java.util.List[StorageFile]]() {}.getType
          gson.fromJson[java.util.List[StorageFile]](new InputStreamReader(in, StandardCharsets.UTF_8), listType).asScala
        }
      } else {
        Seq.empty
      }
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error loading files from $filesFilePath", e)
        Seq.empty
    }
  }

  /**
   * Modify files with proper locking using HDFS lock files
   */
  private def modifyFiles(fn: Seq[StorageFile] => Seq[StorageFile]): Unit = FileBasedMetadata.synchronized {
    // acquire lock by creating a lock file atomically
    var lockAcquired = false
    var retries = 0

    val maxAttempts = MaxLockRetries.toInt.get

    while (!lockAcquired && retries < maxAttempts) {
      try {
        // try to create lock file with overwrite=false for atomicity
        WithClose(fs.create(lockFilePath, false)) { out =>
          // write lock info (hostname, timestamp, etc.)
          val lockInfo = s"${java.net.InetAddress.getLocalHost.getHostName}:${System.currentTimeMillis()}"
          out.write(lockInfo.getBytes(StandardCharsets.UTF_8))
          out.hflush()
          out.hsync()
        }
        lockAcquired = true
      } catch {
        case _: org.apache.hadoop.fs.FileAlreadyExistsException =>
          // lock file exists, check if it's stale
          retries += 1
          if (isLockStale(lockFilePath)) {
            // remove stale lock and retry
            try {
              fs.delete(lockFilePath, false)
            } catch {
              case NonFatal(_) => // ignore, will retry
            }
          } else {
            // wait and retry
            Thread.sleep(LockRetryDelay.toMillis.get)
          }
        case NonFatal(e) =>
          throw new RuntimeException(s"Failed to acquire lock at $lockFilePath", e)
      }
    }

    if (!lockAcquired) {
      throw new RuntimeException(s"Failed to acquire lock after $MaxLockRetries retries")
    }

    try {
      // reload from disk to get latest state
      val currentFiles = buildFileList()

      // apply the modification
      val updatedFiles = fn(currentFiles)

      // write back to disk
      val json = gson.toJson(updatedFiles.asJava)

      WithClose(fs.create(filesFilePath, true)) { out =>
        out.write(json.getBytes(StandardCharsets.UTF_8))
        out.hflush()
        out.hsync()
      }

      // update cache
      filesCache.put(BoxedUnit.UNIT, updatedFiles)

      PathCache.register(fs, filesFilePath)
    } finally {
      // release lock by deleting lock file
      try {
        fs.delete(lockFilePath, false)
      } catch {
        case NonFatal(e) => logger.warn(s"Failed to release lock at $lockFilePath", e)
      }
    }
  }

  /**
   * Check if a lock file is stale (older than lock timeout)
   */
  private def isLockStale(lockPath: Path): Boolean = {
    try {
      val status = fs.getFileStatus(lockPath)
      val age = System.currentTimeMillis() - status.getModificationTime
      age > LockTimeout.toMillis.get
    } catch {
      case NonFatal(_) => true // if we can't read it, consider it stale
    }
  }
}

object FileBasedMetadata {

  val MetadataType = "file"

  // locking parameters
  val MaxLockRetries = SystemProperty("geomesa.fs.metadata.file.lock.retries", "60") // maximum number of lock acquisition retries
  val LockRetryDelay = SystemProperty("geomesa.fs.metadata.file.lock.delay", "1 second") // delay between retries
  val LockTimeout = SystemProperty("geomesa.fs.metadata.file.lock.timeout", "1 minute") // lock timeout

  // gson instance with custom serializers for StorageFile
  private val gson: Gson = StorageMetadata.JsonSerializers.register(new GsonBuilder()).disableHtmlEscaping().create()

  /**
   * Copy a metadata instance. Discards any cached state
   *
   * @param m metadata
   * @return
   */
  def copy(m: FileBasedMetadata): FileBasedMetadata = new FileBasedMetadata(m.fs, m.directory, m.meta)
}
