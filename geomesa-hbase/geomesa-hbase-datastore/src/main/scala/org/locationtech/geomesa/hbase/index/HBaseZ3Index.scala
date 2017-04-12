/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client._
import org.geotools.factory.Hints
import org.locationtech.geomesa.hbase.HBaseFilterStrategyType
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.filters.Z3HBaseFilter
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.ScanConfig
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.index.index.{Z3Index, Z3ProcessingValues}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

case object HBaseZ3Index
    extends HBaseFeatureIndex with Z3Index[HBaseDataStore, HBaseFeature, Mutation, Query] {
  override val version: Int = 1


  /**
    * Sets up everything needed to execute the scan - iterators, column families, deserialization, etc
    *
    * @param sft    simple feature type
    * @param filter hbase filter strategy type
    * @param hints  query hints
    * @param ecql   secondary filter being applied, if any
    * @param dedupe scan may have duplicate results or not
    * @return
    */
  override protected def scanConfig(sft: SimpleFeatureType,
                                    filter: HBaseFilterStrategyType,
                                    hints: Hints,
                                    ecql: Option[Filter],
                                    dedupe: Boolean): ScanConfig = {
    val config = super.scanConfig(sft, filter, hints, ecql, dedupe)

    org.locationtech.geomesa.index.index.Z3Index.currentProcessingValues match {
      case None => config
      case Some(Z3ProcessingValues(sfc, _, xy, _, times)) =>
        // we know we're only going to scan appropriate periods, so leave out whole ones
        val wholePeriod = Seq((sfc.time.min.toLong, sfc.time.max.toLong))
        val filteredTimes = times.filter(_._2 != wholePeriod)
        val normalizedXY = xy.map { case (xmin, ymin, xmax, ymax) =>
          Array(sfc.lon.normalize(xmin), sfc.lat.normalize(ymin),sfc.lon.normalize(xmax), sfc.lat.normalize(ymax))
        }.toArray
        var minEpoch: Int = Short.MaxValue
        var maxEpoch: Int = Short.MinValue
        val tOpts = filteredTimes.toSeq.sortBy(_._1).map { case (bin, times) =>
          times.map { case (t1, t2) =>
            val lt = sfc.time.normalize(t1)
            val ut = sfc.time.normalize(t2)
            if(lt < minEpoch) minEpoch = lt
            if(ut > maxEpoch) maxEpoch = ut
            Array(lt, ut)
          }.toArray
        }.toArray

        // TODO: deal with non-points in the XZ filter
        val filt = new Z3HBaseFilter(new Z3Filter(
          normalizedXY, tOpts, minEpoch.toShort, maxEpoch.toShort, 1, 8)
        )

        config.copy(hbaseFilters = config.hbaseFilters :+ filt)
    }
  }

}
