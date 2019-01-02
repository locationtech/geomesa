/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.legacy

import org.locationtech.geomesa.index.api.WrappedFeature
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.z2.XZ2IndexKeySpace
import org.locationtech.geomesa.index.index.z3.XZ3IndexKeySpace
import org.opengis.feature.simple.SimpleFeatureType

// attribute index with legacy z-curves as secondary index scheme
trait AttributeZIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C]
    extends AttributeShardedIndex[DS, F, W, R, C] {

  override protected def secondaryIndex(sft: SimpleFeatureType): Option[IndexKeySpace[_, _]] =
    Seq(Z3LegacyIndexKeySpace, XZ3IndexKeySpace, Z2LegacyIndexKeySpace, XZ2IndexKeySpace).find(_.supports(sft))
}
