/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.orc

import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.{FileSystemPathReader, FileSystemWriter}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage
import org.locationtech.geomesa.fs.storage.orc.io.{OrcFileSystemReader, OrcFileSystemWriter}

/**
  * Orc implementation of FileSystemStorage
  *
  * @param metadata metadata
  */
class OrcFileSystemStorage(context: FileSystemContext, metadata: StorageMetadata)
    extends AbstractFileSystemStorage(context, metadata, OrcFileSystemStorage.FileExtension) {

  override protected def createWriter(file: Path, observer: FileSystemObserver): FileSystemWriter =
    new OrcFileSystemWriter(metadata.sft, context, file, observer)

  override protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    val optimized = filter.map(FastFilterFactory.optimize(metadata.sft, _))
    new OrcFileSystemReader(metadata.sft, context.conf, optimized, transform)
  }
}

object OrcFileSystemStorage {
  val Encoding      = "orc"
  val FileExtension = "orc"
}
