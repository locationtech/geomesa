/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.filter.index

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex

class SizeSeparatedBucketIndexSupport
    (override val sft: SimpleFeatureType,
     override val index: SizeSeparatedBucketIndex[SimpleFeature]) extends SpatialIndexSupport

object SizeSeparatedBucketIndexSupport {
  def apply(sft: SimpleFeatureType,
            tiers: Seq[(Double, Double)],
            xBucketMultiplier: Double,
            yBucketMultiplier: Double): SizeSeparatedBucketIndexSupport = {
    new SizeSeparatedBucketIndexSupport(sft, new SizeSeparatedBucketIndex(tiers, xBucketMultiplier, yBucketMultiplier))
  }
}
