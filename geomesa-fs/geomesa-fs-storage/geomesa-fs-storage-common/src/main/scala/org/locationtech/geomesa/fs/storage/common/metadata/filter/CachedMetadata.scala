/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata.filter

import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.StorageMetadata
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, StorageFile, StorageFileFilter}

trait CachedMetadata extends StorageMetadata with SchemeFilterExtraction {

  protected def cachedFiles: Seq[StorageFile]

  override def getFiles(): Seq[StorageFile] = cachedFiles

  override def getFiles(partition: Partition): Seq[StorageFile] =
    cachedFiles.filter(_.partition == partition)

  override def getFiles(filter: Filter): Seq[StorageFileFilter] = {
    if (filter == Filter.INCLUDE) {
      getFiles().map(StorageFileFilter(_, None))
    } else {
      val added = scala.collection.mutable.HashSet.empty[StorageFile]
      val files = getFilters(filter).flatMap { f =>
        cachedFiles.collect { case file if
          f.partitionsAnd.forall(p => file.partition.values.exists(v => p.name == v.name && p.contains(v.value))) &&
            // true &&  TODO check spatial/attribute bounds
            added.add(file)
        => StorageFileFilter(file, f.filter)
        }
      }
      logger.debug(s"Matched files:${files.map(f => f.file.file -> f.file.partition).mkString("\n  ", "\n  ", "")}")
      logger.trace(s"Skipped files:${cachedFiles.filterNot(files.map(_.file).contains).map(f => f.file -> f.partition).mkString("\n  ", "\n  ", "")}")
      files
    }
  }
}
