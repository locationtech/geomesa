/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.locationtech.geomesa.fs.storage.api.FileSystemWriter
import org.opengis.feature.simple.SimpleFeature

trait MetadataObservingFileSystemWriter extends FileSystemWriter {
  def metadata: org.locationtech.geomesa.fs.storage.api.FileMetadata

  private var count: Long= 0l
  private var bounds: Envelope = _

  abstract override def write(feature: SimpleFeature): Unit = {
    super.write(feature)
    // Update internal count/bounds/etc
    count += 1
    if (bounds == null) {
      bounds = feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
    } else {
      bounds = {
        bounds.expandToInclude(feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal)
        bounds
      }
    }
//    writeInternal(feature)
  }

//  /**
//    * Implementing classes use this method to write SimpleFeatures to disk
//    * @param feature SimpleFeature to write
//    */
//  def writeInternal(feature: SimpleFeature): Unit

  /**
    * Implementing classes use this method to handle cleaning up writers, etc.
    */
  //def closeInternal(): Unit

  abstract override def close(): Unit = {
    super.close()
    // Finalize metadata
    metadata.incrementFeatureCount(count)
    metadata.expandBounds(bounds)
    metadata.persist()
//    closeInternal()
  }
}
