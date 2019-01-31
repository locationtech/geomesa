/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z2.legacy

import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z2.Z2IndexKeySpace
import org.locationtech.geomesa.index.index.z2.legacy.Z2IndexV1.Z2IndexKeySpaceV1
import org.locationtech.geomesa.index.index.z2.legacy.Z2IndexV3.Z2IndexKeySpaceV3
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// initial implementation - supports points and non-points  (note that non-point support has been removed)
class Z2IndexV1(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, mode: IndexMode)
    extends Z2IndexV3(ds, sft, 1, geom, mode) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val serializedWithId: Boolean = true

  override val keySpace: Z2IndexKeySpace =
    new Z2IndexKeySpaceV1(sft, sft.getTableSharingBytes, ZShardStrategy(sft), geom)
}

object Z2IndexV1 {

  class Z2IndexKeySpaceV1(sft: SimpleFeatureType,
                          sharing: Array[Byte],
                          sharding: ShardStrategy,
                          geomField: String) extends Z2IndexKeySpaceV3(sft, sharing, sharding, geomField) {

    private val serializer = KryoFeatureSerializer(sft) // note: withId

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[Long] = {
      val kv = super.toIndexKey(writable, tier, id, lenient)
      lazy val serialized = serializer.serialize(writable.feature)
      kv.copy(values = kv.values.map(_.copy(cq = Array.empty, toValue = serialized)))
    }
  }
}
