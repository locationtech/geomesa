/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3.legacy

import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV2.Z3IndexKeySpaceV2
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV4.Z3IndexKeySpaceV4
import org.locationtech.geomesa.index.index.z3.{Z3IndexKey, Z3IndexKeySpace}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// non-point support and splits, no table sharing (note that non-point support has been removed)
class Z3IndexV2 protected (ds: GeoMesaDataStore[_],
                           sft: SimpleFeatureType,
                           version: Int,
                           geom: String,
                           dtg: String,
                           mode: IndexMode) extends Z3IndexV4(ds, sft, version, geom, dtg, mode) {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, dtg: String, mode: IndexMode) =
    this(ds, sft, 2, geom, dtg, mode)

  override val serializedWithId: Boolean = true

  override val keySpace: Z3IndexKeySpace = new Z3IndexKeySpaceV2(sft, ZShardStrategy(sft), geom, dtg)
}

object Z3IndexV2 {

  class Z3IndexKeySpaceV2(sft: SimpleFeatureType, sharding: ShardStrategy, geomField: String, dtgField: String)
      extends Z3IndexKeySpaceV4(sft, Array.empty, sharding, geomField, dtgField) {

    private val serializer = KryoFeatureSerializer(sft) // note: withId

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[Z3IndexKey] = {
      val kv = super.toIndexKey(writable, tier, id, lenient)
      lazy val serialized = serializer.serialize(writable.feature)
      kv.copy(values = kv.values.map(_.copy(cq = Array.empty, toValue = serialized)))
    }
  }
}
