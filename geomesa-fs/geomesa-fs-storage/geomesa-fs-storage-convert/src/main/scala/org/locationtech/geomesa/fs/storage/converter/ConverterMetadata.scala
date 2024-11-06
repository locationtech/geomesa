/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionMetadata, StorageFile}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.fs.storage.converter.ConverterStorage.Encoding

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}

class ConverterMetadata(
    context: FileSystemContext,
    val sft: SimpleFeatureType,
    val scheme: PartitionScheme,
    val leafStorage: Boolean
  ) extends StorageMetadata {

  private val dirty = new AtomicBoolean(false)

  private val marker = new Runnable() { override def run(): Unit = dirty.set(true) }
  private val expiry = PathCache.CacheDurationProperty.toDuration.get.toMillis

  private val es = Executors.newSingleThreadScheduledExecutor()

  es.scheduleAtFixedRate(marker, expiry, expiry, TimeUnit.MILLISECONDS)

  override def encoding: String = Encoding

  override def getPartition(name: String): Option[PartitionMetadata] = {
    val path = new Path(context.root, name)
    if (!PathCache.exists(context.fs, path)) { None } else {
      val files = if (leafStorage) { Seq(StorageFile(name, 0L)) } else {
        PathCache.list(context.fs, path).map(fs => StorageFile(fs.getPath.getName, 0L)).toList
      }
      Some(PartitionMetadata(name, files, None, -1L))
    }
  }

  override def getPartitions(prefix: Option[String]): Seq[PartitionMetadata] = {
    buildPartitionList(prefix, dirty.compareAndSet(true, false)).map { name =>
      val files = if (leafStorage) { Seq(StorageFile(name, 0L)) } else {
        PathCache.list(context.fs, new Path(context.root, name)).map(fs => StorageFile(fs.getPath.getName, 0L)).toList
      }
      PartitionMetadata(name, files, None, -1L)
    }
  }

  override def addPartition(partition: PartitionMetadata): Unit =
    throw new UnsupportedOperationException("Converter storage does not support updating metadata")

  override def removePartition(partition: PartitionMetadata): Unit =
    throw new UnsupportedOperationException("Converter storage does not support updating metadata")

  override def setPartitions(partitions: Seq[PartitionMetadata]): Unit =
    throw new UnsupportedOperationException("Converter storage does not support updating metadata")

  override def compact(partition: Option[String], fileSize: Option[Long], threads: Int): Unit =
    throw new UnsupportedOperationException("Converter storage does not support updating metadata")

  override def invalidate(): Unit = dirty.set(true)

  override def close(): Unit = es.shutdown()

  private def buildPartitionList(prefix: Option[String], invalidate: Boolean): List[String] = {
    if (invalidate) {
      PathCache.invalidate(context.fs, context.root)
    }
    val top = PathCache.list(context.fs, context.root)
    top.flatMap(f => buildPartitionList(f.getPath, "", prefix, 1, invalidate)).toList
  }

  private def buildPartitionList(
      path: Path,
      prefix: String,
      filter: Option[String],
      curDepth: Int,
      invalidate: Boolean): List[String] = {
    if (invalidate) {
      PathCache.invalidate(context.fs, path)
    }
    if (curDepth > scheme.depth || !PathCache.status(context.fs, path).isDirectory) {
      val file = s"$prefix${path.getName}"
      if (filter.forall(file.startsWith)) { List(file) } else { List.empty }
    } else {
      val next = s"$prefix${path.getName}"
      val continue = filter.forall { f =>
        if (next.length >= f.length) { next.startsWith(f) } else { next == f.substring(0, next.length) }
      }
      if (continue) {
        PathCache.list(context.fs, path).toList.flatMap { f =>
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
