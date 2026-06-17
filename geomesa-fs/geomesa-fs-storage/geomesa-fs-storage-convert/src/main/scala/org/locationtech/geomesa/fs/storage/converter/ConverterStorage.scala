/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.converter.pathfilter.PathFiltering
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.{FileSystemPathReader, FileSystemUpdateWriter, FileSystemWriter}
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, Partition, StorageMetadata}
import org.locationtech.geomesa.utils.io.CloseWithLogging

class ConverterStorage(
    val context: FileSystemContext,
    val metadata: StorageMetadata,
    converter: SimpleFeatureConverter,
    pathFiltering: Option[PathFiltering]
  ) extends FileSystemStorage {

  override val encoding: String = ConverterStorage.Encoding

  // TODO close converter...
  // the problem is that we aggressively cache storage instances for performance (in FileSystemStorageManager),
  // so even if we wired a 'close' method through the entire storage api, we'd also have to implement a
  // 'borrow/return' paradigm and expire idle instances. Since currently only converters with redis caches
  // actually need to be closed, and since they will only open a single connection per converter, the
  // impact should be low

  override protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    new ConverterFileSystemReader(fs, context.root, converter, filter, transform, pathFiltering)
  }

  override def getWriter(partition: Partition): FileSystemWriter =
    throw new UnsupportedOperationException("Converter storage is read-only")

  override def getWriter(filter: Filter, threads: Int): FileSystemUpdateWriter =
    throw new UnsupportedOperationException("Converter storage is read-only")

  override def close(): Unit = CloseWithLogging(metadata, fs)
}

object ConverterStorage {
  val Encoding = "converter"
}
