/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package metadata

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.PartitionScheme
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.StorageFile
import org.locationtech.geomesa.fs.storage.core.schemes.HierarchicalDateTimeScheme

import java.net.URI
import scala.runtime.BoxedUnit
import scala.util.control.NonFatal

/**
 * Synthetic metadata that just lists files on the filesystem
 *
 * @param context file system
 * @param sft simple feature type
 * @param orderedSchemes partition schemes, ordered according to the file system directory layout
 * @param leafStorage if true, the final partition scheme is a prefix of the file name, instead of a regular directory
 */
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

  filesCache.refresh(BoxedUnit.UNIT) // kick off the initial load asynchronously

  override def addFile(file: StorageFile): Unit = throw new UnsupportedOperationException()
  override def removeFile(file: StorageFile): Unit = throw new UnsupportedOperationException()
  override def replaceFiles(existing: Seq[StorageFile], replacements: Seq[StorageFile]): Unit =
    throw new UnsupportedOperationException()

  override protected def buildFileList(): Seq[StorageFile] = {
    logger.debug("Building file list")
    val start = System.currentTimeMillis()
    val result = Seq.newBuilder[StorageFile]
    try {
      val toCheck = new java.util.LinkedList[URI]()
      context.fs.list(context.root).foreach(toCheck.add)
      val maxDepth = depth + context.root.toString.count(_ == '/')
      val rootPathLength = context.root.toString.length
      while (!toCheck.isEmpty) {
        val file = toCheck.remove()
        logger.debug(s"Checking ${file.getPath}")
        if (file.getPath.endsWith("/")) {
          if (file.toString.count(_ == '/') <= maxDepth) {
            context.fs.list(file).foreach(toCheck.add)
          }
        } else {
          val relativePath = file.toString.substring(rootPathLength)
          var partitionParts = relativePath.split("/")
          if (leafStorage) {
            partitionParts(partitionParts.length - 1) = partitionParts.last.takeWhile(_ != '_')
          } else {
            partitionParts = partitionParts.dropRight(1)
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
