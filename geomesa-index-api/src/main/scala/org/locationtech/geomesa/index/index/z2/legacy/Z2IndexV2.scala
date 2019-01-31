/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z2.legacy

import java.nio.charset.StandardCharsets

import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api.{RowKeyValue, ShardStrategy, WritableFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z2.Z2IndexKeySpace
import org.locationtech.geomesa.index.index.z2.legacy.Z2IndexV2.Z2IndexKeySpaceV2
import org.locationtech.geomesa.index.index.z2.legacy.Z2IndexV3.Z2IndexKeySpaceV3
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// deprecated non-point support in favor of xz, ids in row key, per-attribute vis
class Z2IndexV2(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, mode: IndexMode)
    extends Z2IndexV3(ds, sft, 2, geom, mode) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val keySpace: Z2IndexKeySpace =
    new Z2IndexKeySpaceV2(sft, sft.getTableSharingBytes, ZShardStrategy(sft), geom)
}

object Z2IndexV2 {

  // in 1.2.5/6, we stored the CQ as the number of rows, which was always one
  // this wasn't captured correctly in a versioned index class, so we need to delete both possible CQs
  private val deleteCq = "1,".getBytes(StandardCharsets.UTF_8)

  class Z2IndexKeySpaceV2(sft: SimpleFeatureType,
                          sharing: Array[Byte],
                          sharding: ShardStrategy,
                          geomField: String) extends Z2IndexKeySpaceV3(sft, sharing, sharding, geomField) {

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[Long] = {
      val kv = super.toIndexKey(writable, tier, id, lenient)
      // lenient means we're deleting...
      if (lenient) { kv.copy(values = kv.values ++ kv.values.map(_.copy(cq = deleteCq))) } else { kv }
    }
  }
}
