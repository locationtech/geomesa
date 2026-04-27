/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package utils

import org.locationtech.geomesa.fs.storage.core.Metadata
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.fs.storage.core.utils.FileSize.UpdatingFileSizeEstimator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.FileSizeEstimator

import java.io.Closeable
import java.net.URI

/**
 * Utility for tracking target file sizes
 *
 * @param fs filesystem - note, not cleaned up, must be closed externally
 * @param metadata metadata
 */
class FileSize(fs: ObjectStore, metadata: StorageMetadata) {

  private val fileSizeError = FileSize.FileSizeErrorThreshold.toFloat.get

  private var averageBytesPerFeature = metadata.get(FileSize.BytesPerFeature) match {
    case Some(b) => b.toFloat
    case None    => (metadata.sft.getAttributeCount + 1) * 1.6f // 1.6 taken from some sample data estimates...
  }

  /**
   * Gets the target file size
   *
   * @return
   */
  def targetSize: Option[Long] = metadata.get(Metadata.TargetFileSize).map(_.toLong)

  /**
   * Check if a file is already the desired size
   *
   * @param path file path
   * @param target target file size
   * @return true if the file is appropriately sized
   */
  def fileIsSized(path: URI, target: Long): Boolean = {
    val size = fs.size(path)
    math.abs((size.toDouble / target) - 1d) <= fileSizeError
  }

  /**
   * Gets a file size estimator for this storage instance
   *
   * @param size target file size
   * @return
   */
  def estimator(size: Long): UpdatingFileSizeEstimator =
    FileSize.this.synchronized {
      new UpdatingFileSizeEstimator(size, this)
    }

  private def updateFileSize(bytesPerFeature: Float): Unit = {
    if (metadata.get(FileSize.UseDynamicSizing).forall(_.toBoolean)) {
      synchronized {
        if (math.abs((bytesPerFeature / averageBytesPerFeature) - 1f) > fileSizeError) {
          metadata.set(FileSize.BytesPerFeature, java.lang.Float.toString(bytesPerFeature))
          averageBytesPerFeature = bytesPerFeature
        }
      }
    }
  }
}

object FileSize {

  val BytesPerFeature  = "bytes-per-feature"
  val UseDynamicSizing = "use-dynamic-sizing"

  val FileSizeErrorThreshold: SystemProperty = SystemProperty("geomesa.fs.size.threshold", "0.05")

  class UpdatingFileSizeEstimator(target: Long, instance: FileSize)
    extends FileSizeEstimator(target, instance.fileSizeError, instance.averageBytesPerFeature) with Closeable {
    override def close(): Unit = getBytesPerFeature.foreach(instance.updateFileSize)
  }
}
