/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata.filter

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.StorageMetadata
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, StorageFile}
import org.locationtech.geomesa.fs.storage.common.metadata.filter.SchemeFilterExtraction.SchemeFilter
import org.locationtech.geomesa.fs.storage.common.utils.PathCache

import java.util.concurrent.TimeUnit
import scala.runtime.BoxedUnit

trait CachedMetadata extends StorageMetadata with SchemeFilterExtraction {

  protected val filesCache: LoadingCache[BoxedUnit, Seq[StorageFile]] =
    Caffeine.newBuilder().refreshAfterWrite(PathCache.CacheDurationProperty.toDuration.get.toMillis, TimeUnit.MILLISECONDS).build(
      new CacheLoader[BoxedUnit, Seq[StorageFile]]() {
        override def load(key: BoxedUnit): Seq[StorageFile] = buildFileList()
      }
    )

  private def cachedFiles: Seq[StorageFile] = filesCache.get(BoxedUnit.UNIT)

  protected def buildFileList(): Seq[StorageFile]

  override def getFiles(): Seq[StorageFile] = cachedFiles

  override def getFiles(partition: Partition): Seq[StorageFile] =
    cachedFiles.filter(_.partition == partition)

  override def getFiles(filter: Filter): Seq[StorageFile] = {
    if (filter == Filter.INCLUDE) {
      getFiles()
    } else {
      val added = scala.collection.mutable.HashSet.empty[StorageFile]
      val files = getFilters(filter).flatMap { f =>
        cachedFiles.collect { case file if matches(file, f) && added.add(file) => file }
      }
      logger.debug(s"Matched files:${files.mkString("\n  ", "\n  ", "")}")
      logger.trace(s"Skipped files:${cachedFiles.filterNot(files.map(_.file).contains).mkString("\n  ", "\n  ", "")}")
      files
    }
  }

  override def close(): Unit = {
    val refresh = filesCache.policy().refreshes().get(BoxedUnit.UNIT)
    if (refresh != null && !refresh.isDone) {
      refresh.cancel(true)
    }
  }

  private def matches(file: StorageFile, f: SchemeFilter): Boolean =
    f.partitions.forall(p => file.partition.values.exists(v => p.name == v.name && p.contains(v.value))) &&
      f.spatialBounds.values.forall(or => file.spatialBounds.find(b => b.attribute == or.attribute).forall(b => or.bounds.exists(_.intersects(b)))) &&
      f.attributeBounds.values.forall(or => file.attributeBounds.find(b => b.attribute == or.attribute).forall(b => or.bounds.exists(_.intersects(b))))

}
