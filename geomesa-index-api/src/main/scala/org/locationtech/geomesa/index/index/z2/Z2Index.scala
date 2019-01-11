/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z2

import org.locationtech.geomesa.index.api.WrappedFeature
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.index.{BaseFeatureIndex, ShardStrategy}
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.opengis.feature.simple.SimpleFeatureType

trait Z2Index[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C]
    extends BaseFeatureIndex[DS, F, W, R, C, Z2IndexValues, Long] with SpatialFilterStrategy[DS, F, W] {

  override val name: String = Z2Index.Name

  override protected val keySpace: Z2IndexKeySpace = Z2IndexKeySpace

  override protected def shardStrategy(sft: SimpleFeatureType): ShardStrategy = ZShardStrategy(sft)
}

object Z2Index {
  val Name = "z2"
}
