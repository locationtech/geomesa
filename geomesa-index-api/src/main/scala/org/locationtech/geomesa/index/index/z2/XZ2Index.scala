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

trait XZ2Index[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C]
    extends BaseFeatureIndex[DS, F, W, R, C, XZ2IndexValues, Long] with SpatialFilterStrategy[DS, F, W] {

  override val name: String = XZ2Index.Name

  override protected val keySpace: XZ2IndexKeySpace = XZ2IndexKeySpace

  override protected def shardStrategy(sft: SimpleFeatureType): ShardStrategy = ZShardStrategy(sft)
}

object XZ2Index {
  val Name = "xz2"
}
