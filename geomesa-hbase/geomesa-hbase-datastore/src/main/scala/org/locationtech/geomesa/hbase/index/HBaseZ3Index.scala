/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.hbase.HBaseFilterStrategyType
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.filters.Z3HBaseFilter
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.index.index.{Z3Index, Z3ProcessingValues}
import org.opengis.feature.simple.SimpleFeatureType

case object HBaseZ3Index extends HBaseLikeZ3Index with HBasePlatform {

  override protected def createPushDownFilters(ds: HBaseDataStore,
                                               sft: SimpleFeatureType,
                                               filter: HBaseFilterStrategyType,
                                               transform: Option[(String, SimpleFeatureType)]): Seq[(Int, HFilter)] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val z3Filter = Z3Index.currentProcessingValues.map { case Z3ProcessingValues(sfc, _, xy, _, times) =>
      val offset = if (sft.isTableSharing) { 2 } else { 1 } // sharing + shard - note: currently sharing is always false
      configureZ3PushDown(sfc, xy, times, offset)
    }
    super.createPushDownFilters(ds, sft, filter, transform) ++ z3Filter.toSeq
  }

  private def configureZ3PushDown(sfc: Z3SFC,
                                  xy: Seq[(Double, Double, Double, Double)],
                                  timeMap: Map[Short, Seq[(Long, Long)]],
                                  offset: Int): (Int, HFilter) = {
    // we know we're only going to scan appropriate periods, so leave out whole ones
    val wholePeriod = Seq((sfc.time.min.toLong, sfc.time.max.toLong))
    val filteredTimes = timeMap.filter(_._2 != wholePeriod)
    val normalizedXY = xy.map { case (xmin, ymin, xmax, ymax) =>
      Array(sfc.lon.normalize(xmin), sfc.lat.normalize(ymin), sfc.lon.normalize(xmax), sfc.lat.normalize(ymax))
    }.toArray
    var minEpoch: Int = Short.MaxValue
    var maxEpoch: Int = Short.MinValue
    val tOpts = filteredTimes.toSeq.sortBy(_._1).map { case (bin, times) =>
      times.map { case (t1, t2) =>
        val lt = sfc.time.normalize(t1)
        val ut = sfc.time.normalize(t2)
        if (lt < minEpoch) minEpoch = lt
        if (ut > maxEpoch) maxEpoch = ut
        Array(lt, ut)
      }.toArray
    }.toArray

    val filter = new Z3HBaseFilter(new Z3Filter(normalizedXY, tOpts, minEpoch.toShort, maxEpoch.toShort, 8), offset)
    (Z3HBaseFilter.Priority, filter)
  }
}

trait HBaseLikeZ3Index extends HBaseFeatureIndex with Z3Index[HBaseDataStore, HBaseFeature, Mutation, Query]  {
  override val version: Int = 1
}
