/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.iterators.StatsScan.{StatResult, StatsResultsToFeatures}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
 * Reads simple features and observe them with a Stat server-side
 *
 * Only works with z3IdxStrategy for now (queries that date filters)
 */
class StatsIterator extends BaseAggregatingIterator[StatResult] with StatsScan

object StatsIterator extends LazyLogging {

  val DEFAULT_PRIORITY = 30

  // Need a filler namespace, else geoserver throws NPE for xml output

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val is = new IteratorSetting(priority, "stats-iter", classOf[StatsIterator])
    StatsScan.configure(sft, index, filter, hints).foreach { case (k, v) => is.addOption(k, v) }
    is
  }

  /**
    * Adapts the iterator to create simple features.
    */
  class AccumuloStatsResultsToFeatures extends StatsResultsToFeatures[Entry[Key, Value]] {
    override protected def bytes(result: Entry[Key, Value]): Array[Byte] = result.getValue.get()
  }
}
