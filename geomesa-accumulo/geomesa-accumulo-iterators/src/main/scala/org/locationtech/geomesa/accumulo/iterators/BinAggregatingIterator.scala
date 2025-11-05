/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.BinAggregatingScan
import org.locationtech.geomesa.index.iterators.BinAggregatingScan.{BinResultsToFeatures, ResultCallback}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

import java.util.Map.Entry

/**
 * Iterator that computes and aggregates 'bin' entries
 */
class BinAggregatingIterator extends BaseAggregatingIterator[ResultCallback] with BinAggregatingScan

object BinAggregatingIterator extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  val DEFAULT_PRIORITY = 25

  /**
   * Configure based on query hints
   */
  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val trackId = hints.getBinTrackIdField
    val geom = hints.getBinGeomField.getOrElse(sft.getGeomField)
    val dtg = hints.getBinDtgField.orElse(sft.getDtgField)
    val label = hints.getBinLabelField
    val batchSize = hints.getBinBatchSize
    val sort = hints.isBinSorting
    val sampling = hints.getSampling

    val is = new IteratorSetting(priority, "bin-iter", classOf[BinAggregatingIterator])
    BinAggregatingScan.configure(sft, index, filter, trackId, geom, dtg, label, batchSize, sort, sampling).foreach {
      case (k, v) => is.addOption(k, v)
    }
    is
  }

  /**
    * Adapts the iterator to create simple features
    */
  class AccumuloBinResultsToFeatures extends BinResultsToFeatures[Entry[Key, Value]] {
    override protected def bytes(result: Entry[Key, Value]): Array[Byte] = result.getValue.get()
  }
}
