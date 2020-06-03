/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.fs.storage.orc.utils.OrcAttributeWriter
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

class OrcFileSystemWriter(
    sft: SimpleFeatureType,
    config: Configuration,
    file: Path,
    observer: FileSystemObserver = NoOpObserver
  ) extends FileSystemWriter {

  private val schema = OrcFileSystemStorage.createTypeDescription(sft)

  private val options = OrcFile.writerOptions(config).setSchema(schema)
  private val writer = OrcFile.createWriter(file, options)
  private val batch = schema.createRowBatch()

  private val attributeWriter = OrcAttributeWriter(sft, batch)

  override def write(sf: SimpleFeature): Unit = {
    attributeWriter.apply(sf, batch.size)
    batch.size += 1
    // If the batch is full, write it out and start over
    if (batch.size == batch.getMaxSize) {
      writer.addRowBatch(batch)
      batch.reset()
    }
    observer.write(sf)
  }

  override def flush(): Unit = {
    flushBatch()
    observer.flush()
  }

  override def close(): Unit = {
    try { flushBatch() } catch {
      case NonFatal(e) => CloseQuietly(Seq(writer, observer)).foreach(e.addSuppressed); throw e
    }
    CloseQuietly.raise(Seq(writer, observer))
  }

  private def flushBatch(): Unit = {
    if (batch.size != 0) {
      writer.addRowBatch(batch)
      batch.reset()
    }
  }
}
