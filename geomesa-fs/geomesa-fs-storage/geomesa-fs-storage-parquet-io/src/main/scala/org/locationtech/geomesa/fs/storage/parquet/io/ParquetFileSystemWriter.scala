/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.parquet.conf.ParquetConfiguration
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.utils.io.CloseQuietly

import java.net.URI

/**
 * Parquet writer
 *
 * @param file file to write
 * @param conf configuration, must include the feature type encoded according to `SimpleFeatureParquetSchema`
 * @param observer any observers
 */
class ParquetFileSystemWriter(
    file: URI,
    conf: ParquetConfiguration,
    observer: FileSystemObserver = NoOpObserver
  ) extends FileSystemWriter {

  private val writer = SimpleFeatureParquetWriter.builder(file, conf).build()

  def getDataSize: Long = writer.getDataSize

  override def write(f: SimpleFeature): Unit = {
    writer.write(f)
    observer(f)
  }

  override def flush(): Unit = observer.flush()

  override def close(): Unit = CloseQuietly(Seq(writer, observer)).foreach(e => throw e)
}
