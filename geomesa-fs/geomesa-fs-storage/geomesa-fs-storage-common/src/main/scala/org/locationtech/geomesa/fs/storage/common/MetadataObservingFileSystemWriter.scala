/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.geomesa.fs.storage.api.FileSystemWriter
import org.locationtech.geomesa.fs.storage.common.MetadataFileSystemStorage.WriterCallback
import org.opengis.feature.simple.SimpleFeature

trait MetadataObservingFileSystemWriter extends FileSystemWriter {

  def callback: WriterCallback

  private var count: Long = 0L
  private val bounds: Envelope = new Envelope()

  abstract override def write(feature: SimpleFeature): Unit = {
    super.write(feature)
    // Update internal count/bounds/etc
    count += 1L
    bounds.expandToInclude(feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal)
  }

  abstract override def close(): Unit = {
    super.close()
    callback.onClose(count, bounds)
  }
}
