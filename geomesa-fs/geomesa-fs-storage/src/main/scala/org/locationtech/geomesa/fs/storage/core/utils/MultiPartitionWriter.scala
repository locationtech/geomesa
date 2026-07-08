/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.utils

import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, Partition}

/**
 * Multi partition writer
 *
 * @param storage storage instance
 * @param maxOpenPartitions max open partition writers
 */
class MultiPartitionWriter(storage: FileSystemStorage, maxOpenPartitions: Int)
    extends MultiPartitionAction[FileSystemWriter](storage, maxOpenPartitions) with FileSystemWriter {

  override def write(feature: SimpleFeature): Unit = apply(feature)

  override def size: Long = throw new UnsupportedOperationException("MultiPartitionWriter creates multiple files")

  override protected def apply(writer: FileSystemWriter, feature: SimpleFeature): Unit = writer.write(feature)
  override protected def createAction(partition: Partition): FileSystemWriter = storage.getWriter(partition)
}
