/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.legacy

import org.locationtech.geomesa.curve.LegacyZ3SFC
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.api.WrappedFeature
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z3.{Z3Index, Z3IndexKeySpace}

trait Z3LegacyIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C] extends Z3Index[DS, F, W, R, C] {
  override protected val keySpace = Z3LegacyIndexKeySpace
}

object Z3LegacyIndexKeySpace extends Z3IndexKeySpace {
  override def sfc(period: TimePeriod) = LegacyZ3SFC(period)
}
