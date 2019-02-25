/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.{FileSystemPathReader, WriterCallback}
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class ConverterStorage(context: FileSystemContext, metadata: StorageMetadata, converter: SimpleFeatureConverter)
    extends AbstractFileSystemStorage(context, metadata, "") {

  // TODO close converter...
  // the problem is that we aggressively cache storage instances for performance (in FileSystemStorageManager),
  // so even if we wired a 'close' method through the entire storage api, we'd also have to implement a
  // 'borrow/return' paradigm and expire idle instances. Since currently only converters with redis caches
  // actually need to be closed, and since they will only open a single connection per converter, the
  // impact should be low

  override protected def createWriter(file: Path, callback: WriterCallback): FileSystemWriter =
    throw new NotImplementedError()

  override protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    new ConverterFileSystemReader(context.fc, converter, filter, transform)
  }

  override def getFilePaths(partition: String): Seq[Path] = {
    val path = new Path(context.root, partition)
    if (metadata.leafStorage) { Seq(path) } else {
      PathCache.list(context.fc, path).map(_.getPath).toList
    }
  }

  override def getWriter(partition: String): FileSystemWriter =
    throw new UnsupportedOperationException("Converter storage does not support feature writing")

  override def compact(partition: Option[String], threads: Int): Unit =
    throw new UnsupportedOperationException("Converter storage does not support compactions")
}

object ConverterStorage {
  val Encoding = "converter"
}
