/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package metadata

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import org.apache.iceberg.DataFile
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.core.metadata.SchemeFilterExtraction.{ColumnOr, SchemeFilter}

import java.util.concurrent.TimeUnit
import scala.runtime.BoxedUnit

/**
 * Metadata mixin that caches a list of files in memory, with automatic background refresh. Useful for when
 * determining the list of files is expensive.
 */
trait CachedMetadata extends StorageMetadata with SchemeFilterExtraction {

  protected val filesCache: LoadingCache[BoxedUnit, Seq[DataFile]] =
    Caffeine.newBuilder().refreshAfterWrite(CacheDurationProperty.toDuration.get.toMillis, TimeUnit.MILLISECONDS).build(
      new CacheLoader[BoxedUnit, Seq[DataFile]]() {
        override def load(key: BoxedUnit): Seq[DataFile] = buildFileList()
      }
    )

  private def cachedFiles: Seq[DataFile] = filesCache.get(BoxedUnit.UNIT)

  protected def buildFileList(): Seq[DataFile]

  /**
   * Extract Partition from a DataFile. Implementations should override this to use their IcebergMapper.
   */
  protected def extractPartition(file: DataFile): Partition

  override def getFiles(): Seq[DataFile] = cachedFiles

  override def getFiles(partition: Partition): Seq[DataFile] =
    cachedFiles.filter(f => extractPartition(f) == partition)

  override def getFiles(filter: Filter): Seq[DataFile] = {
    if (filter == Filter.INCLUDE) {
      getFiles()
    } else {
      val added = scala.collection.mutable.HashSet.empty[DataFile]
      val files = getFilters(filter).flatMap { f =>
        cachedFiles.collect { case file if matches(file, f) && added.add(file) => file }
      }
      logger.debug(s"Matched files:${files.mkString("\n  ", "\n  ", "")}")
      logger.trace(s"Skipped files:${cachedFiles.filterNot(files.map(_.location()).contains).mkString("\n  ", "\n  ", "")}")
      files
    }
  }

  override def close(): Unit = {
    val refresh = filesCache.policy().refreshes().get(BoxedUnit.UNIT)
    if (refresh != null && !refresh.isDone) {
      refresh.cancel(true)
    }
  }

  private def matches(file: DataFile, f: SchemeFilter): Boolean = {
    val partition = extractPartition(file)
    matches(partition, f.partitions) && matches(file, f.columnBounds)
  }

  private def matches(partition: Partition, partitions: Seq[PartitionRange]): Boolean =
    partitions.forall(p => partition.values.exists(v => p.name == v.name && p.contains(v.value)))

  private def matches(file: DataFile, columnBounds: Seq[ColumnOr])(implicit d0: DummyImplicit): Boolean = {
    // TODO: implement bounds checking using DataFile.lowerBounds() / upperBounds()
    // For now, include all files (conservative - may read extra files but won't miss any)
    true
  }
}
