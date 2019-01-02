/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3.legacy

import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z3.Z3IndexKeySpace
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV2.Z3IndexKeySpaceV2
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// initial z3 implementation - only supports points
class Z3IndexV1(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, dtg: String, mode: IndexMode)
    extends Z3IndexV2(ds, sft, 1, geom, dtg, mode) {
  override val keySpace: Z3IndexKeySpace = new Z3IndexKeySpaceV2(sft, NoShardStrategy, geom, dtg)
}
