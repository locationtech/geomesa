/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3.legacy

import org.locationtech.geomesa.curve.{LegacyZ3SFC, Z3SFC}
import org.locationtech.geomesa.index.api.ShardStrategy
import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.LegacyTableNaming
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV4.Z3IndexKeySpaceV4
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV5.Z3IndexKeySpaceV5
import org.locationtech.geomesa.index.index.z3.{Z3IndexKey, Z3IndexKeySpace, Z3IndexValues}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// legacy z curve - normal table sharing
class Z3IndexV4 protected (ds: GeoMesaDataStore[_],
                           sft: SimpleFeatureType,
                           version: Int,
                           geom: String,
                           dtg: String,
                           mode: IndexMode)
    extends Z3IndexV5(ds, sft, version, geom, dtg, mode) with LegacyTableNaming[Z3IndexValues, Z3IndexKey] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, dtg: String, mode: IndexMode) =
    this(ds, sft, 4, geom, dtg, mode)

  override val keySpace: Z3IndexKeySpace =
    new Z3IndexKeySpaceV4(sft, sft.getTableSharingBytes, ZShardStrategy(sft), geom, dtg)
}

object Z3IndexV4 {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  class Z3IndexKeySpaceV4(sft: SimpleFeatureType,
                          sharing: Array[Byte],
                          sharding: ShardStrategy,
                          geomField: String,
                          dtgField: String)
      extends Z3IndexKeySpaceV5(sft, sharing, sharding, geomField, dtgField) {
    override protected val sfc: Z3SFC = LegacyZ3SFC(sft.getZ3Interval)
  }
}
