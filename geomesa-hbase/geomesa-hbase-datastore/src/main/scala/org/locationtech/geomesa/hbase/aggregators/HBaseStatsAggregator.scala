/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.aggregators

import org.geotools.util.factory.Hints
import org.locationtech.geomesa.hbase.rpc.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.iterators.StatsScan.StatsResultsToFeatures
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object HBaseStatsAggregator {

  import org.locationtech.geomesa.hbase.data.HBaseIndexAdapter.AggregatorPackage

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints): Map[String, String] = {
    StatsScan.configure(sft, index, filter, hints) +
      (GeoMesaCoprocessor.AggregatorClass -> s"$AggregatorPackage.HBaseStatsAggregator")
  }

  class HBaseStatsResultsToFeatures extends StatsResultsToFeatures[Array[Byte]] {
    override protected def bytes(result: Array[Byte]): Array[Byte] = result
  }
}
