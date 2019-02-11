/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z2.legacy

import org.locationtech.geomesa.curve.{LegacyZ2SFC, Z2SFC}
import org.locationtech.geomesa.index.api.ShardStrategy
import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.LegacyTableNaming
import org.locationtech.geomesa.index.index.z2.legacy.Z2IndexV3.Z2IndexKeySpaceV3
import org.locationtech.geomesa.index.index.z2.legacy.Z2IndexV4.Z2IndexKeySpaceV4
import org.locationtech.geomesa.index.index.z2.{Z2IndexKeySpace, Z2IndexValues}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// legacy z curve - no delete checks for old col qualifiers
class Z2IndexV3 protected (ds: GeoMesaDataStore[_], sft: SimpleFeatureType, version: Int, geom: String, mode: IndexMode)
    extends Z2IndexV4(ds, sft, version, geom, mode) with LegacyTableNaming[Z2IndexValues, Long] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, mode: IndexMode) =
    this(ds, sft, 3, geom, mode)

  override val keySpace: Z2IndexKeySpace =
    new Z2IndexKeySpaceV3(sft, sft.getTableSharingBytes, ZShardStrategy(sft), geom)
}

object Z2IndexV3 {
  class Z2IndexKeySpaceV3(sft: SimpleFeatureType,
                          sharing: Array[Byte],
                          sharding: ShardStrategy,
                          geomField: String) extends Z2IndexKeySpaceV4(sft, sharing, sharding, geomField) {
    override protected val sfc: Z2SFC = LegacyZ2SFC
  }
}
