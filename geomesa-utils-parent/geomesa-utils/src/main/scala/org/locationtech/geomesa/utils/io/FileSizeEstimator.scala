/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import com.typesafe.scalalogging.LazyLogging

/**
 * Estimates how many features to write to create a file of a target size
 *
 * @param target target file size, in bytes
 * @param error acceptable percent error for file size, in bytes
 * @param estimatedBytesPerFeature initial estimate for bytes per feature
 */
class FileSizeEstimator(target: Long, error: Float, estimatedBytesPerFeature: Float) extends LazyLogging {

  require(error >= 0 && error < 1f, "Error must be a percentage between [0,1)")

  private val threshold = math.round(target * error.toDouble)
  private var estimate = estimatedBytesPerFeature.toDouble
  private var updatedEstimate: Option[Float] = None

  /**
   * Estimate how many features to write to hit our target size
   *
   * @param written number of bytes written so far
   * @return
   */
  def estimate(written: Long): Long = math.max(1L, math.round((target - written) / estimate))

  /**
   * Re-evaluate the bytes per feature, based on having written out a certain number of features
   *
   * @param size size of the file created
   * @param count number of features written to the file
   */
  def update(size: Long, count: Long): Unit = {
    if (size > 0 && count > 0 && math.abs(size - target) > threshold) {
      val update = size.toDouble / count
      logger.debug(s"Updating bytesPerFeature from $estimate to $update based on writing $count features in $size bytes")
      estimate = update
      updatedEstimate = Some(estimate.toFloat)
    } else {
      logger.debug(s"Not updating bytesPerFeature from $estimate based on writing $count features in $size bytes")
    }
  }

  /**
   * Returns the bytes per feature, based on data written so far
   *
   * @return
   */
  def getBytesPerFeature: Option[Float] = updatedEstimate

  /**
   * Checks if the bytes written is (at least) within the error threshold of the desired size
   *
   * @param size size of the file created
   * @return
   */
  def done(size: Long): Boolean = size > target || math.abs(size - target) < threshold
}
