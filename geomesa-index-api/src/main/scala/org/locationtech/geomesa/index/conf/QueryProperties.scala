/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

object QueryProperties {

  val QueryExactCount           : SystemProperty = SystemProperty("geomesa.force.count", "false")
  val QueryExactCountMaxFeatures: SystemProperty = SystemProperty("geomesa.exact.count.max.features", "1000")
  val QueryCostType             : SystemProperty = SystemProperty("geomesa.query.cost.type")
  val QueryTimeout              : SystemProperty = SystemProperty("geomesa.query.timeout") // default is no timeout
  val QueryThreads              : SystemProperty = SystemProperty("geomesa.query.threads")

  // rough upper limit on the number of ranges we will generate per query
  val ScanRangesTarget: SystemProperty = SystemProperty("geomesa.scan.ranges.target", "2000")

  // decomposition is disabled by default
  val PolygonDecompMultiplier: SystemProperty = SystemProperty("geomesa.query.decomposition.multiplier", "0")
  val PolygonDecompBits: SystemProperty = SystemProperty("geomesa.query.decomposition.bits", "20")

  val SortMemoryThreshold: SystemProperty = SystemProperty("geomesa.sort.memory.threshold")

  // S2 parameter configuration
  private val S2CoverConfig = SystemProperty("google.s2.coverer.config", "0,30,1,8").get.split(",").map(_.trim.toInt)

  val S2MinLevel: Int = S2CoverConfig(0)
  val S2MaxLevel: Int = S2CoverConfig(1)
  val S2LevelMod: Int = S2CoverConfig(2)
  val S2MaxCells: Int = S2CoverConfig(3)

  // allow for full table scans or preempt them due to size of data set
<<<<<<< HEAD
  val BlockFullTableScans = SystemProperty("geomesa.scan.block-full-table", "false")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2aefae6145 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 05abd783e4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
<<<<<<< HEAD
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 2aefae6145 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 825515ce74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 2aefae6145 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
  val BlockFullTableScans = new SystemProperty("geomesa.scan.block-full-table", "false") {
    @deprecated("Deprecated with no replacement")
    def onFullTableScan(typeName: String, filter: Filter): Unit = {
      val block = blockFullTableScansForFeatureType(typeName).orElse(toBoolean).getOrElse(false)
      if (block) {
        throw new RuntimeException(s"Full-table scans are disabled. Query being stopped for $typeName: " +
            org.locationtech.geomesa.filter.filterToString(filter))
      }
    }
  }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
=======
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 4c943341c0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 2aefae6145 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 05abd783e4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c7527e3843 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> a3651df0c4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 9ddab1e4ef (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> ee55e5ab74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 825515ce74 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 8c519079d0 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 1f18a80880 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 34b2d6632f (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9e5293be2 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 83be617313 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 19265e7a87 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 9f601af8cc (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
=======
>>>>>>> 9e5293be2a (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> 05abd783e4 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
>>>>>>> bc58bb16eb (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> 6e41de2827 (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 8e01f6263e (GEOMESA-3214 Fix warning about full table scan with Filter.EXCLUDE)

  val BlockMaxThreshold: SystemProperty = SystemProperty("geomesa.scan.block-full-table.threshold", "1000")

  /**
   * Is the system property for a specific feature type set
   *
   * @param typeName type name
   * @return
   */
  def blockFullTableScansForFeatureType(typeName: String): Option[Boolean] = {
    Option(GeoMesaSystemProperties.getProperty(s"geomesa.scan.$typeName.block-full-table"))
        .map(java.lang.Boolean.parseBoolean)
  }
}
