/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.Map.Entry

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.ArrowEncodedSft
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, QueryPlan}
import org.locationtech.geomesa.index.iterators.ArrowScan
import org.locationtech.geomesa.index.iterators.ArrowScan.{ArrowAggregate, ArrowScanConfig}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
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
  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                stats: GeoMesaStats,
                filter: Option[Filter],
                ecql: Option[Filter],
                hints: Hints,
                priority: Int = DefaultPriority): (IteratorSetting, QueryPlan.Reducer) = {
    val is = new IteratorSetting(priority, "arrow-iter", classOf[ArrowIterator])
    val ArrowScanConfig(config, reduce) = ArrowScan.configure(sft, index, stats, filter, ecql, hints)
    config.foreach { case (k, v) => is.addOption(k, v) }
    (is, reduce)
  }

  /**
    * Adapts the iterator to create simple features.
    * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
    */
  def kvsToFeatures(): Entry[Key, Value] => SimpleFeature = {
    val sf = new ScalaSimpleFeature(ArrowEncodedSft, "")
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    e: Entry[Key, Value] => {
      sf.setAttribute(0, e.getValue.get())
      sf
    }
  }
}

