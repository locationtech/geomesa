/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloQueryPlannerType
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.index.iterators.{DensityScan, StatsScan}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Executes a query against geomesa
 */
class AccumuloQueryPlanner(ds: AccumuloDataStore) extends AccumuloQueryPlannerType(ds) {

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
    } else if (hints.isMapAggregatingQuery) {
      val spec = KryoLazyMapAggregatingIterator.createMapSft(sft, hints.getMapAggregatingAttribute)
      SimpleFeatureTypes.createType(sft.getTypeName, spec)
    } else {
      super.getReturnSft(sft, hints)
    }
  }
}
