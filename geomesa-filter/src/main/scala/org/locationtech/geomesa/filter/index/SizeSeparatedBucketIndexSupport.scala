/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.index

import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
