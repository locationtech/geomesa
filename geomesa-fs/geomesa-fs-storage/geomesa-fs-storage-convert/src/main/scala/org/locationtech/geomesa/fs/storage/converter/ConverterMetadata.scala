/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionMetadata
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.fs.storage.converter.ConverterStorage.Encoding
import org.opengis.feature.simple.SimpleFeatureType

class ConverterMetadata(
    context: FileSystemContext,
    val sft: SimpleFeatureType,
    val scheme: PartitionScheme,
    val leafStorage: Boolean
  ) extends StorageMetadata {

  private val dirty = new AtomicBoolean(false)

  override def encoding: String = Encoding

  override def getPartition(name: String): Option[PartitionMetadata] = {
    val path = new Path(context.root, name)
    if (!PathCache.exists(context.fc, path)) { None } else {
      val files = if (leafStorage) { Seq(name) } else {
        PathCache.list(context.fc, path).map(_.getPath.getName).toList
      }
      Some(PartitionMetadata(name, files, None, -1L))
    }
  }

  override def getPartitions(prefix: Option[String]): Seq[PartitionMetadata] = {
    buildPartitionList(prefix, dirty.compareAndSet(true, false)).map { name =>
      val files = if (leafStorage) { Seq(name) } else {
        PathCache.list(context.fc, new Path(context.root, name)).map(_.getPath.getName).toList
      }
      PartitionMetadata(name, files, None, -1L)
    }
  }

  override def reload(): Unit = dirty.set(true)

  override def addPartition(partition: PartitionMetadata): Unit =
    throw new UnsupportedOperationException("Converter storage does not support updating metadata")

  override def removePartition(partition: PartitionMetadata): Unit =
    throw new UnsupportedOperationException("Converter storage does not support updating metadata")

  override def compact(partition: Option[String], threads: Int): Unit =
    throw new UnsupportedOperationException("Converter storage does not support updating metadata")

  override def close(): Unit = {}

  private def buildPartitionList(prefix: Option[String], invalidate: Boolean): List[String] = {
    if (invalidate) {
      PathCache.invalidate(context.fc, context.root)
    }
    val top = PathCache.list(context.fc, context.root)
    top.flatMap(f => buildPartitionList(f.getPath, "", prefix, 1, invalidate)).toList
  }

  private def buildPartitionList(
      path: Path,
      prefix: String,
      filter: Option[String],
      curDepth: Int,
      invalidate: Boolean): List[String] = {
    if (invalidate) {
      PathCache.invalidate(context.fc, path)
    }
    if (curDepth > scheme.depth || !PathCache.status(context.fc, path).isDirectory) {
      val file = s"$prefix${path.getName}"
      if (filter.forall(file.startsWith)) { List(file) } else { List.empty }
    } else {
      val next = s"$prefix${path.getName}"
      val continue = filter.forall { f =>
        if (next.length >= f.length) { next.startsWith(f) } else { next == f.substring(0, next.length) }
      }
      if (continue) {
        PathCache.list(context.fc, path).toList.flatMap { f =>
          buildPartitionList(f.getPath, s"$next/", filter, curDepth + 1, invalidate)
        }
      } else {
        List.empty
      }
    }
  }
}

object ConverterMetadata {
  val Name = "converter"
}
