/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.conf

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.uuid.Z3FeatureIdGenerator

object QueryProperties {
  val QUERY_EXACT_COUNT    = SystemProperty("geomesa.force.count", "false")
  val QUERY_COST_TYPE      = SystemProperty("geomesa.query.cost.type", null)
  val QUERY_TIMEOUT_MILLIS = SystemProperty("geomesa.query.timeout.millis", null) // default is no timeout
  // rough upper limit on the number of ranges we will generate per query
  val SCAN_RANGES_TARGET   = SystemProperty("geomesa.scan.ranges.target", "2000")
}

object FeatureIdProperties {
  val FEATURE_ID_GENERATOR =
    SystemProperty("geomesa.feature.id-generator", classOf[Z3FeatureIdGenerator].getCanonicalName)
}
