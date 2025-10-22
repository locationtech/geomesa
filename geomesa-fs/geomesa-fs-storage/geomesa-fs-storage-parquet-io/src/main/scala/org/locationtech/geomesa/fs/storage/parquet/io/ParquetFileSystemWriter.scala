/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.api.FileSystemContext
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.io.CloseQuietly

/**
 * Parquet writer
 *
 * @param sft simple feature type
 * @param context context
 * @param file file to write
 * @param observer any observers
 */
class ParquetFileSystemWriter(
    sft: SimpleFeatureType,
    context: FileSystemContext,
    file: Path,
    observer: FileSystemObserver = NoOpObserver
  ) extends FileSystemWriter {

  private val conf = {
    val conf = new Configuration(context.conf)
    StorageConfiguration.setSft(conf, sft)
    conf
  }

  private val writer = SimpleFeatureParquetWriter.builder(file, conf).build()

  override def write(f: SimpleFeature): Unit = {
    writer.write(f)
    observer(f)
  }

  override def flush(): Unit = observer.flush()

  override def close(): Unit = {
    CloseQuietly(Seq(writer, observer)).foreach(e => throw e)
    PathCache.register(context.fs, file)
    // TODO
//    if (FileValidationEnabled.toBoolean.get) {
//      validateParquetFile(file)
//    }
  }
}
