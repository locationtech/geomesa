/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package utils

import org.apache.iceberg.{DataFile, Table}
import org.locationtech.geomesa.fs.storage.core.Metadata
import org.locationtech.geomesa.fs.storage.core.utils.FileSize.UpdatingFileSizeEstimator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.FileSizeEstimator

import java.io.Closeable
import java.net.URI

/**
 * Utility for tracking target file sizes
 *
 * @param table table, not cleaned up, must be closed externally
 */
class FileSize(table: Table) {

  private val fileSizeError = FileSize.FileSizeErrorThreshold.toFloat.get

  private var averageBytesPerFeature = Metadata.get(table, FileSize.BytesPerFeature) match {
    case Some(b) => b.toFloat
    case None    => table.schema().columns().size() * 1.6f // 1.6 taken from some sample data estimates...
  }

  /**
   * Gets the target file size
   *
   * @return
   */
  def targetSize: Option[Long] = Metadata.get(table, Metadata.TargetFileSize).map(_.toLong)

  /**
   * Check if a file is already the desired size
   *
   * @param path file path
   * @param target target file size
   * @return true if the file is appropriately sized
   */
  def fileIsSized(path: URI, target: Long): Boolean = isSized(table.io().newInputFile(path.toString).getLength, target)

  /**
   * Check if a file is already the desired size
   *
   * @param file file
   * @param target target file size
   * @return true if the file is appropriately sized
   */
  def fileIsSized(file: DataFile, target: Long): Boolean = isSized(file.fileSizeInBytes(), target)

  /**
   * Compare target with actual size, allowing for some margin of error
   *
   * @param size file size
   * @param target target size
   * @return
   */
  private def isSized(size: Long, target: Long): Boolean = math.abs((size.toDouble / target) - 1d) <= fileSizeError

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
    if (Metadata.get(table, FileSize.UseDynamicSizing).forall(_.toBoolean)) {
      synchronized {
        if (math.abs((bytesPerFeature / averageBytesPerFeature) - 1f) > fileSizeError) {
          averageBytesPerFeature = bytesPerFeature
          Metadata.set(table, FileSize.BytesPerFeature, java.lang.Float.toString(bytesPerFeature))
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
