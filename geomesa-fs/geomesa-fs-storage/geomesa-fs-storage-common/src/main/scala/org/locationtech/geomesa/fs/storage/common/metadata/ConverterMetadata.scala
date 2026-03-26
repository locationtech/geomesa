/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileStatus, Path}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, PartitionKey, StorageFile}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.metadata.filter.CachedMetadata
import org.locationtech.geomesa.fs.storage.common.partitions.HierarchicalDateTimeScheme
import org.locationtech.geomesa.fs.storage.common.utils.PathCache

import java.util.concurrent.TimeUnit
import scala.runtime.BoxedUnit
import scala.util.control.NonFatal

class ConverterMetadata(
    context: FileSystemContext,
    val sft: SimpleFeatureType,
    val orderedSchemes: Seq[PartitionScheme],
    val leafStorage: Boolean
  ) extends StorageMetadata with CachedMetadata with LazyLogging {

  override val `type`: String = ConverterMetadata.MetadataType

  override val schemes: Set[PartitionScheme] = orderedSchemes.toSet

  private val depth = orderedSchemes.map {
    case s: HierarchicalDateTimeScheme => s.depth
    case _ => 1
  }.sum - (if (leafStorage) { 1 } else { 0 })

  private val filesCache =
    Caffeine.newBuilder().refreshAfterWrite(PathCache.CacheDurationProperty.toDuration.get.toMillis, TimeUnit.MILLISECONDS).build(
      new CacheLoader[BoxedUnit, Seq[StorageFile]]() {
        override def load(key: BoxedUnit): Seq[StorageFile] = buildFileList()
        override def reload(key: BoxedUnit, oldValue: Seq[StorageFile]): Seq[StorageFile] = {
          load(key) // TODO could optimize this?
        }
      }
    )

  private val refresh = filesCache.refresh(BoxedUnit.UNIT)

  override protected def cachedFiles: Seq[StorageFile] = filesCache.get(BoxedUnit.UNIT)

  override def addFile(file: StorageFile): Unit = throw new UnsupportedOperationException()
  override def removeFile(file: StorageFile): Unit = throw new UnsupportedOperationException()
  override def replaceFiles(existing: Seq[StorageFile], replacements: Seq[StorageFile]): Unit =
    throw new UnsupportedOperationException()

// TODO?  override def invalidate(): Unit = dirty.set(true)

  override def close(): Unit = if (!refresh.isDone) { refresh.cancel(true) }

  private def buildFileList(): Seq[StorageFile] = {
    logger.debug("Building file list")
    val start = System.currentTimeMillis()
    val result = Seq.newBuilder[StorageFile]
    try {
      val toCheck = new java.util.LinkedList[FileStatus]()
      PathCache.list(context.fs, context.root).foreach(toCheck.add)
      val maxDepth = depth + context.root.depth()
      val rootPathLength = context.fs.resolvePath(context.root).toString.length
      while (!toCheck.isEmpty) {
        val status = toCheck.remove()
        logger.debug(s"Checking ${status.getPath}")
        if (status.isDirectory) {
          if (status.getPath.depth() <= maxDepth) {
            PathCache.list(context.fs, status.getPath).foreach(toCheck.add)
          }
        } else {
          val relativePath = status.getPath.toString.substring(rootPathLength + 1)
          var partitionParts = relativePath.split("/")
          if (leafStorage) {
            partitionParts = partitionParts :+ new Path(relativePath).getName.takeWhile(_ != '_')
          }
          var i = 0
          val partitions = orderedSchemes.map {
            case s: HierarchicalDateTimeScheme =>
              val key = PartitionKey(s.name, partitionParts.slice(i, i + s.depth).mkString("/"))
              i += s.depth
              key
            case s =>
              val key = PartitionKey(s.name, partitionParts(i))
              i += 1
              key
          }
          result += StorageFile(relativePath, Partition(partitions.toSet), -1L)
        }
      }
    } catch {
      case NonFatal(e) => logger.error("Error building file list:", e); throw e
    }
    val list = result.result()
    logger.debug(s"Found ${list.size} files in ${System.currentTimeMillis() - start}ms")
    logger.whenTraceEnabled(list.foreach(f => logger.trace(f.toString)))
    list.foreach(f => logger.debug(f.toString))
    list
  }
}

object ConverterMetadata {
  val MetadataType = "converter"

  val ConverterPathParam   = "fs.options.converter.path"
  val SftNameParam         = "fs.options.sft.name"
  val SftConfigParam       = "fs.options.sft.conf"
  val LeafStorageParam     = "fs.options.leaf-storage"
  val PartitionSchemeParam = "fs.partition-scheme.name"
  val PartitionOptsPrefix  = "fs.partition-scheme.opts."
}
