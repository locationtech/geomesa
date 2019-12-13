/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3.legacy

import java.nio.charset.StandardCharsets

import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV3.Z3IndexKeySpaceV3
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV4.Z3IndexKeySpaceV4
import org.locationtech.geomesa.index.index.z3.{Z3IndexKey, Z3IndexKeySpace}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// deprecated polygon support in favor of xz, ids in row key, per-attribute vis
class Z3IndexV3 protected (ds: GeoMesaDataStore[_],
                           sft: SimpleFeatureType,
                           version: Int,
                           geom: String,
                           dtg: String,
                           mode: IndexMode) extends Z3IndexV4(ds, sft, version, geom, dtg, mode) {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, dtg: String, mode: IndexMode) =
    this(ds, sft, 3, geom, dtg, mode)

  // note: this index doesn't ever have table sharing
  override val keySpace: Z3IndexKeySpace = new Z3IndexKeySpaceV3(sft, ZShardStrategy(sft), geom, dtg)
}

object Z3IndexV3 {

  // in 1.2.5/6, we stored the CQ as the number of rows, which was always one
  // this wasn't captured correctly in a versioned index class, so we need to delete both possible CQs
  private val deleteCq = "1,".getBytes(StandardCharsets.UTF_8)

  class Z3IndexKeySpaceV3(sft: SimpleFeatureType, sharding: ShardStrategy, geomField: String, dtgField: String)
      extends Z3IndexKeySpaceV4(sft, Array.empty, sharding, geomField, dtgField) {

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[Z3IndexKey] = {
      val kv = super.toIndexKey(writable, tier, id, lenient)
      // lenient means we're deleting...
      if (lenient) { kv.copy(values = kv.values ++ kv.values.map(_.copy(cq = deleteCq))) } else { kv }
    }
  }
}
