/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/
package org.locationtech.geomesa.utils.geotools

class RangeSnap(val interval: com.google.common.collect.Range[java.lang.Long], val buckets: Int) {

  val bucketSize: Long = (interval.upperEndpoint - interval.lowerEndpoint) / buckets

  /**
   * Computes the bucket where the given value falls under
   * @param value the time ordinate
   * @return bucket for histogram
   */
  def getBucket(value: Long): Long =
    if (interval.contains(value)) {
      val bucketIndex = (value - interval.lowerEndpoint) / bucketSize
      interval.lowerEndpoint + (bucketSize * bucketIndex.toInt)
    } else if (value > interval.upperEndpoint) {
      interval.upperEndpoint
    } else {
      interval.lowerEndpoint
    }
}