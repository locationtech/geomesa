/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.uuid.Z3FeatureIdGenerator

object FeatureProperties {
  val FEATURE_ID_GENERATOR =
    SystemProperty("geomesa.feature.id-generator", classOf[Z3FeatureIdGenerator].getCanonicalName)
}
