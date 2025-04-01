/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.index

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.utils.index.BucketIndex

class BucketIndexSupport(override val sft: SimpleFeatureType, override val index: BucketIndex[SimpleFeature])
    extends SpatialIndexSupport {
  override def toString: String = s"BucketIndexSupport[${sft.getTypeName}:${index.xBuckets}x${index.yBuckets}]"
}

object BucketIndexSupport {
  def apply(sft: SimpleFeatureType, xResolution: Int, yResolution: Int): BucketIndexSupport =
    new BucketIndexSupport(sft, new BucketIndex(xResolution, yResolution))
}
