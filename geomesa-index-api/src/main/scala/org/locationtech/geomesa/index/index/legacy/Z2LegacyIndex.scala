/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.legacy

import org.locationtech.geomesa.curve.LegacyZ2SFC
import org.locationtech.geomesa.index.api.WrappedFeature
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexKeySpace}

trait Z2LegacyIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C] extends Z2Index[DS, F, W, R, C] {
  override protected val keySpace = Z2LegacyIndexKeySpace
}

object Z2LegacyIndexKeySpace extends Z2IndexKeySpace {
  override val sfc = LegacyZ2SFC
}
