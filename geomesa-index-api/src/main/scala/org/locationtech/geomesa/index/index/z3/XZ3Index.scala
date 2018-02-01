/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3

import org.geotools.factory.Hints
import org.locationtech.geomesa.index.api.{FilterStrategy, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.BaseFeatureIndex
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.opengis.feature.simple.SimpleFeatureType

trait XZ3Index[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C]
    extends BaseFeatureIndex[DS, F, W, R, C, XZ3IndexValues] with SpatioTemporalFilterStrategy[DS, F, W] {

  override val name: String = XZ3Index.Name

  override protected val keySpace: XZ3IndexKeySpace = XZ3IndexKeySpace

  // always apply the full filter to xz queries
  override protected def useFullFilter(sft: SimpleFeatureType,
                                       ds: DS,
                                       filter: FilterStrategy[DS, F, W],
                                       indexValues: Option[XZ3IndexValues],
                                       hints: Hints): Boolean = true
}

object XZ3Index {
  val Name = "xz3"
}
