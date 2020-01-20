/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.Map.Entry

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.api.QueryPlan.FeatureReducer
import org.locationtech.geomesa.index.iterators.ArrowScan
import org.locationtech.geomesa.index.iterators.ArrowScan.{ArrowAggregate, ArrowResultsToFeatures, ArrowScanConfig}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class ArrowIterator extends BaseAggregatingIterator[ArrowAggregate] with ArrowScan

object ArrowIterator {

  val DefaultPriority = 25

  /**
    * Configure the iterator
    *
    * @param sft simple feature type
    * @param index feature index being run against
    * @param stats handle to stats, may be used to dictionary creation
    * @param filter full filter, may be used for dictionary creation
    * @param ecql secondary filter, applied to the rows processed by the scan
    * @param hints query hints
    * @param priority iterator priority
    * @return
    */
  def configure(
      sft: SimpleFeatureType,
      index: GeoMesaFeatureIndex[_, _],
      stats: GeoMesaStats,
      filter: Option[Filter],
      ecql: Option[Filter],
      hints: Hints,
      priority: Int = DefaultPriority): (IteratorSetting, FeatureReducer) = {
    val is = new IteratorSetting(priority, "arrow-iter", classOf[ArrowIterator])
    val ArrowScanConfig(config, reduce) = ArrowScan.configure(sft, index, stats, filter, ecql, hints)
    config.foreach { case (k, v) => is.addOption(k, v) }
    (is, reduce)
  }

  /**
    * Adapts the iterator to create simple features.
    */
  class AccumuloArrowResultsToFeatures extends ArrowResultsToFeatures[Entry[Key, Value]] {
    override protected def bytes(result: Entry[Key, Value]): Array[Byte] = result.getValue.get()
  }
}

