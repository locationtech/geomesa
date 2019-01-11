/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.filter.Filter

object QueryProperties {

  val QueryExactCount = SystemProperty("geomesa.force.count", "false")
  val QueryCostType   = SystemProperty("geomesa.query.cost.type")
  val QueryTimeout    = SystemProperty("geomesa.query.timeout") // default is no timeout

  // rough upper limit on the number of ranges we will generate per query
  val ScanRangesTarget = SystemProperty("geomesa.scan.ranges.target", "2000")

  // decomposition is disabled by default
  val PolygonDecompMultiplier = SystemProperty("geomesa.query.decomposition.multiplier", "0")
  val PolygonDecompBits = SystemProperty("geomesa.query.decomposition.bits", "20")

  // noinspection TypeAnnotation
  // allow for full table scans or preempt them due to size of data set
  val BlockFullTableScans = new SystemProperty("geomesa.scan.block-full-table", "false") {
    def onFullTableScan(typeName: String, filter: Filter): Unit = {
      val block =
        Option(GeoMesaSystemProperties.getProperty(s"geomesa.scan.$typeName.block-full-table"))
          .map(java.lang.Boolean.parseBoolean)
          .orElse(toBoolean)
          .getOrElse(false)
      if (block) {
        throw new RuntimeException(s"Full-table scans are disabled. Query being stopped for $typeName: " +
            org.locationtech.geomesa.filter.filterToString(filter))
      }
    }
  }

  val BlockMaxThreshold = SystemProperty("geomesa.scan.block-full-table.threshold", "1000")
}
