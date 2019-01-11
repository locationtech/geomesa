/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.id

import org.locationtech.geomesa.index.api.WrappedFeature
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.index.{BaseFeatureIndex, ShardStrategy}
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.opengis.feature.simple.SimpleFeatureType

trait IdIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C]
    extends BaseFeatureIndex[DS, F, W, R, C, Set[Array[Byte]], Array[Byte]] with IdFilterStrategy[DS, F, W] {

  override val name: String = IdIndex.Name

  override def supports(sft: SimpleFeatureType): Boolean = true

  override protected val keySpace: IdIndexKeySpace = IdIndexKeySpace

  override protected def shardStrategy(sft: SimpleFeatureType): ShardStrategy = NoShardStrategy
}

object IdIndex {
  val Name = "id"
}
