/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.uuid.Z3FeatureIdGenerator

object FeatureProperties {
  val FEATURE_ID_GENERATOR =
    SystemProperty("geomesa.feature.id-generator", classOf[Z3FeatureIdGenerator].getCanonicalName)
}
