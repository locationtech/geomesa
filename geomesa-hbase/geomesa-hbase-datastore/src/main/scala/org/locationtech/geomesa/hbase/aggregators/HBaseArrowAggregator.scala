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
import org.locationtech.geomesa.index.iterators.ArrowScan
import org.locationtech.geomesa.index.iterators.ArrowScan.{ArrowResultsToFeatures, ArrowScanConfig}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object HBaseArrowAggregator {

  import org.locationtech.geomesa.hbase.data.HBaseIndexAdapter.AggregatorPackage

  /**
    * Configure the aggregator
    *
    * @param sft simple feature type
    * @param index feature index
    * @param stats stats, used for querying dictionaries
    * @param filter full filter from the query, if any
    * @param ecql secondary push down filter, if any
    * @param hints query hints
    * @return
    */
  def configure(
      sft: SimpleFeatureType,
      index: GeoMesaFeatureIndex[_, _],
      stats: GeoMesaStats,
      filter: Option[Filter],
      ecql: Option[Filter],
      hints: Hints): ArrowScanConfig = {
    val conf = ArrowScan.configure(sft, index, stats, filter, ecql, hints)
    conf.copy(conf.config + (GeoMesaCoprocessor.AggregatorClass -> s"$AggregatorPackage.HBaseArrowAggregator"))
  }

  class HBaseArrowResultsToFeatures extends ArrowResultsToFeatures[Array[Byte]] {
    override protected def bytes(result: Array[Byte]): Array[Byte] = result
  }
}
