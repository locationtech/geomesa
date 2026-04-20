/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.converter.pathfilter.PathFiltering
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.{FileSystemPathReader, FileSystemWriter}
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.StorageFile
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, Partition, StorageMetadata}

import java.net.URI

class ConverterStorage(
    context: FileSystemContext,
    metadata: StorageMetadata,
    converter: SimpleFeatureConverter,
    pathFiltering: Option[PathFiltering]
  ) extends FileSystemStorage(context, metadata, "") {

  override val encoding: String = ConverterStorage.Encoding

  // TODO close converter...
  // the problem is that we aggressively cache storage instances for performance (in FileSystemStorageManager),
  // so even if we wired a 'close' method through the entire storage api, we'd also have to implement a
  // 'borrow/return' paradigm and expire idle instances. Since currently only converters with redis caches
  // actually need to be closed, and since they will only open a single connection per converter, the
  // impact should be low

  override protected def createWriter(file: URI, partition: Partition, observer: FileSystemObserver): FileSystemWriter =
    throw new UnsupportedOperationException()

  override protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    new ConverterFileSystemReader(context.fs, context.root, converter, filter, transform, pathFiltering)
  }

  override def compact(partition: Partition, fileSize: Option[Long], threads: Int): Unit =
    throw new UnsupportedOperationException("Converter storage does not support compactions")

  override def register(file: URI): StorageFile =
    throw new UnsupportedOperationException("Converter storage does not support file registration")
}

object ConverterStorage {
  val Encoding = "converter"
}
