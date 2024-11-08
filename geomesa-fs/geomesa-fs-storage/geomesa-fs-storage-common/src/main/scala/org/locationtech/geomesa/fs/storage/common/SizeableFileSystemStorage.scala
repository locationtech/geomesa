/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, Metadata}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.FileSizeEstimator

trait SizeableFileSystemStorage extends FileSystemStorage {

  private val fileSizeError = SizeableFileSystemStorage.FileSizeErrorThreshold.toFloat.get

  private var averageBytesPerFeature = metadata.get(SizeableFileSystemStorage.BytesPerFeature) match {
    case Some(b) => b.toFloat
    case None    => (metadata.sft.getAttributeCount + 1) * 1.6f // 1.6 taken from some sample data estimates...
  }

  /**
   * Gets the target file size
   *
   * @param size override default target size
   * @return
   */
  def targetSize(size: Option[Long]): Option[Long] =
    size.orElse(metadata.get(Metadata.TargetFileSize).map(_.toLong))

  /**
   * Check if a file is already the desired size
   *
   * @param path file path
   * @param target target file size
   * @return true if the file is appropriately sized
   */
  def fileIsSized(path: Path, target: Long): Boolean = {
    val size = context.fs.getFileStatus(path).getLen
    math.abs((size.toDouble / target) - 1d) <= fileSizeError
  }

  /**
   * Gets a file size estimator for this storage instance
   *
   * @param size target file size
   * @return
   */
  def estimator(size: Long): FileSizeEstimator = synchronized {
    new FileSizeEstimator(size, fileSizeError, averageBytesPerFeature)
  }

  def updateFileSize(estimator: FileSizeEstimator): Unit = {
    estimator.getBytesPerFeature.foreach { b =>
      if (metadata.get(SizeableFileSystemStorage.UseDynamicSizing).forall(_.toBoolean)) {
        synchronized {
          if (math.abs((b / averageBytesPerFeature) - 1f) > fileSizeError) {
            metadata.set(SizeableFileSystemStorage.BytesPerFeature, java.lang.Float.toString(b))
            averageBytesPerFeature = b
          }
        }
      }
    }
  }
}

object SizeableFileSystemStorage {

  val BytesPerFeature  = "bytes-per-feature"
  val UseDynamicSizing = "use-dynamic-sizing"

  val FileSizeErrorThreshold: SystemProperty = SystemProperty("geomesa.fs.size.threshold", "0.05")
}
