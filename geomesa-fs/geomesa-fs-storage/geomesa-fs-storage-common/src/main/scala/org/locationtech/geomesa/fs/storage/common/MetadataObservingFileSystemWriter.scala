/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.locationtech.geomesa.fs.storage.api.FileSystemWriter
import org.opengis.feature.simple.SimpleFeature

abstract class MetadataObservingFileSystemWriter extends FileSystemWriter {
  def metadata: org.locationtech.geomesa.fs.storage.api.FileMetadata

  override def write(feature: SimpleFeature): Unit = {
    // Update internal count/bounds/etc
    writeInternal(feature)
  }

  def writeInternal(feature: SimpleFeature): Unit

  override def close(): Unit = {
    // Finalize metadata
  }
}
