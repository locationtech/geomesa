/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package metadata

import com.google.gson._
import com.google.gson.reflect.TypeToken
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.{ColumnBounds, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.fs.storage.core.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.StringSerialization

import java.io.{InputStreamReader, OutputStreamWriter}
import java.lang.reflect.Type
import java.net.URI
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
class FileBasedMetadata(fs: ObjectStore, meta: Metadata, directory: URI)
    extends StorageMetadata with CachedMetadata with LazyLogging {

  import FileBasedMetadata._

  import scala.collection.JavaConverters._

  require(directory.toString.endsWith("/"), "Invalid path - must end with '/'")

  private val kvs = new ConcurrentHashMap[String, String](meta.config.asJava)
  private val encodedTypeName = StringSerialization.alphaNumericSafeString(meta.sft.getTypeName)
  private val metadataFilePath = directory.resolve(s"$encodedTypeName.json")
  private val filesFilePath = directory.resolve(s".${encodedTypeName}_files.json")
  private val lockFilePath = directory.resolve(s".${encodedTypeName}_files.json.lock")

  override val `type`: String = FileBasedMetadata.MetadataType

  override val sft: SimpleFeatureType = meta.sft
  override val schemes: Set[PartitionScheme] = meta.partitions.map(PartitionSchemeFactory.load(sft, _)).toSet

  filesCache.refresh(BoxedUnit.UNIT) // kick off the initial load asynchronously

  override def get(key: String): Option[String] = Option(kvs.get(key))

  // note: this isn't synchronized across jvms
  override def set(key: String, value: String): Unit = FileBasedMetadata.synchronized {
    if (value == null) {
      kvs.remove(key)
    } else {
      kvs.put(key, value)
    }
    WithClose(fs.overwrite(metadataFilePath)) { out =>
      MetadataSerialization.serialize(out, meta.copy(config = kvs.asScala.toMap))
    }
  }

  override def addFile(file: StorageFile): Unit = {
    modifyFiles { files =>
      // remove any existing file with the same path and add the new one
      (files.filterNot(_.file == file.file) :+ file).sortBy(_.timestamp)(Ordering.Long.reverse)
    }
    logger.debug(s"Added file $file")
  }

  override def removeFile(file: StorageFile): Unit = {
    modifyFiles { files =>
      files.filterNot(_.file == file.file)
    }
    logger.debug(s"Removed file $file")
  }

  override def replaceFiles(existing: Seq[StorageFile], replacements: Seq[StorageFile]): Unit = {
    val existingFiles = existing.map(_.file)
    modifyFiles { files =>
      files.filterNot(f => existingFiles.contains(f.file)) ++ replacements
    }
    logger.debug(s"Replaced ${existing.size} files with ${replacements.size} new ones")
  }

  override def close(): Unit = try { super.close() } finally { fs.close() }

  /**
   * Load files from the JSON file
   */
  override protected def buildFileList(): Seq[StorageFile] = {
    WithClose(fs.read(filesFilePath)) { opt =>
      opt.fold(Seq.empty[StorageFile]) { is =>
        val listType = new TypeToken[java.util.List[StorageFile]]() {}.getType
        gson.fromJson[java.util.List[StorageFile]](new InputStreamReader(is, StandardCharsets.UTF_8), listType).asScala.toSeq
      }
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
      logger.trace(s"Attempting to acquire lock at $lockFilePath with ${maxAttempts - retries} tries left")
      try {
        // try to create lock file with overwrite=false for atomicity
        try {
          fs.create(lockFilePath).foreach { out =>
            try {
              // write lock info for debugging - hostname + timestamp
              val lockInfo = s"${java.net.InetAddress.getLocalHost.getHostName}:${System.currentTimeMillis()}"
              out.write(lockInfo.getBytes(StandardCharsets.UTF_8))
            } finally {
              out.close()
            }
            lockAcquired = true
          }
        } catch {
          case NonFatal(e) => logger.debug("Error writing lock file, may already exist?", e)
        }

        if (!lockAcquired) {
          retries += 1
          if (retries < maxAttempts) {
            // check if lockfile is stale
            fs.modified(lockFilePath).foreach { modified =>
              val age = System.currentTimeMillis() - modified
              if (age > LockTimeout.toMillis.get) {
                logger.debug(s"Deleting expired lock file (age ${age}ms at $lockFilePath")
                fs.delete(lockFilePath)
              }
            }
            // wait and retry
            logger.debug(s"Could not acquire lock - waiting for ${LockRetryDelay.toMillis.get}ms before next attempt")
            Thread.sleep(LockRetryDelay.toMillis.get)
          }
        }
      } catch {
        case NonFatal(e) => throw new RuntimeException(s"Failed to acquire lock at $lockFilePath", e)
      }
    }

    if (!lockAcquired) {
      throw new RuntimeException(s"Failed to acquire lock after $MaxLockRetries retries")
    }
    logger.debug(s"Acquired lock file at $lockFilePath")

    try {
      // reload from disk to get latest state
      val currentFiles = buildFileList()

      // apply the modification
      val updatedFiles = fn(currentFiles)

      // write back to disk
      WithClose(new OutputStreamWriter(fs.overwrite(filesFilePath), StandardCharsets.UTF_8)) { writer =>
        gson.toJson(updatedFiles.asJava, writer)
      }

      // update cache
      filesCache.put(BoxedUnit.UNIT, updatedFiles)
    } finally {
      // release lock by deleting lock file
      try {
        fs.delete(lockFilePath)
        logger.debug(s"Released lock file at $lockFilePath")
      } catch {
        case NonFatal(e) => logger.warn(s"Failed to release lock at $lockFilePath", e)
      }
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
  private val gson: Gson =
    new GsonBuilder()
      .registerTypeAdapter(classOf[PartitionKey], PartitionKey.PartitionKeySerializer)
      .registerTypeAdapter(classOf[Partition], Partition.PartitionSerializer)
      .registerTypeAdapter(classOf[StorageFile], StorageFileSerializer)
      .disableHtmlEscaping()
      .create()

  /**
   * Json serializer for StorageFileAction
   */
  private object StorageFileSerializer extends JsonSerializer[StorageFile] with JsonDeserializer[StorageFile] {

    import scala.collection.JavaConverters._

    override def serialize(src: StorageFile, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
      val obj = new JsonObject()
      obj.addProperty("file", src.file)
      obj.add("partition", context.serialize(src.partition))
      obj.addProperty("count", src.count)
      obj.addProperty("action", src.action.toString)
      val bounds = new JsonArray(src.bounds.size)
      src.bounds.foreach(b => bounds.add(context.serialize(b)))
      obj.add("bounds", bounds)
      val sort = new JsonArray(src.sort.size)
      src.sort.foreach(sort.add(_))
      obj.add("sort", sort)
      obj.addProperty("timestamp", src.timestamp)
      obj
    }

    override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): StorageFile = {
      val obj = json.getAsJsonObject
      StorageFile(
        obj.getAsJsonPrimitive("file").getAsString,
        context.deserialize(obj.get("partition"), classOf[Partition]),
        obj.getAsJsonPrimitive("count").getAsLong,
        StorageFileAction.withName(obj.getAsJsonPrimitive("action").getAsString),
        obj.getAsJsonArray("bounds").asList().asScala.map(context.deserialize[ColumnBounds](_, classOf[ColumnBounds])).toSeq,
        obj.getAsJsonArray("sort").asList().asScala.map(_.getAsInt).toSeq,
        obj.getAsJsonPrimitive("timestamp").getAsLong,
      )
    }
  }
}
