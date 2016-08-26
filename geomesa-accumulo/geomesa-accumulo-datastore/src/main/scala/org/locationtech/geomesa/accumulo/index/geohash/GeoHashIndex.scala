/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.geohash

import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.opengis.feature.simple.SimpleFeatureType

@deprecated("z2/z3")
object GeoHashIndex extends AccumuloFeatureIndex with GeoHashWritableIndex with GeoHashQueryableIndex {

  override val name: String = "st_idx"

  override val version: Int = 1

  override def supports(sft: SimpleFeatureType): Boolean = sft.getGeometryDescriptor != null
}
