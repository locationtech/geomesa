/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.orc.utils.OrcAttributeWriter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class OrcFileSystemWriter(sft: SimpleFeatureType, config: Configuration, file: Path) extends FileSystemWriter {

  private val schema = OrcFileSystemStorage.createTypeDescription(sft)

  private val options = OrcFile.writerOptions(config).setSchema(schema)
  private val writer = OrcFile.createWriter(file, options)

  private val batch: VectorizedRowBatch = schema.createRowBatch()

  private val attributeWriter = OrcAttributeWriter(sft, batch)

  override def write(sf: SimpleFeature): Unit = {
    attributeWriter.apply(sf, batch.size)
    batch.size += 1
    // If the batch is full, write it out and start over
    if (batch.size == batch.getMaxSize) {
      writer.addRowBatch(batch)
      batch.reset()
    }
  }

  override def flush(): Unit = {
    if (batch.size != 0) {
      writer.addRowBatch(batch)
      batch.reset()
    }
  }

  override def close(): Unit = {
    flush()
    writer.close()
  }
}
