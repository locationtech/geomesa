/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import org.geotools.factory.Hints
import org.locationtech.geomesa.index.iterators.{DensityScan, StatsScan}
import org.locationtech.geomesa.kudu.KuduQueryPlannerType
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.opengis.feature.simple.SimpleFeatureType

class KuduQueryPlanner(ds: KuduDataStore) extends KuduQueryPlannerType(ds) {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  // This function calculates the SimpleFeatureType of the returned SFs.
  override protected [geomesa] def getReturnSft(sft: SimpleFeatureType, hints: Hints): SimpleFeatureType = {
    if (hints.isBinQuery) {
      BinaryOutputEncoder.BinEncodedSft
    } else if (hints.isArrowQuery) {
      org.locationtech.geomesa.arrow.ArrowEncodedSft
    } else if (hints.isDensityQuery) {
      DensityScan.DensitySft
    } else if (hints.isStatsQuery) {
      StatsScan.StatsSft
    } else {
      super.getReturnSft(sft, hints)
    }
  }
}
