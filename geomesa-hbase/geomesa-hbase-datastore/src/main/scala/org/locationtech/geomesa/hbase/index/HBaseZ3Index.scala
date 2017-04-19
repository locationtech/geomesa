/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.hbase.HBaseFilterStrategyType
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.filters.{JSimpleFeatureFilter, Z3HBaseFilter}
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.ScanConfig
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.index.index.{Z3Index, Z3ProcessingValues}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

case object HBaseZ3Index extends HBaseLikeZ3Index with HBasePlatform {

  override def configurePushDownFilters(config: ScanConfig,
                                        ecql: Option[Filter],
                                        transform: Option[(String, SimpleFeatureType)],
                                        sft: SimpleFeatureType): ScanConfig = {
    // TODO: currently we configure both z3 filter and ECQL geometry/time push down
    // TODO: optimize by removing geo and time predicates from ECQL

    val z3Filter =
      org.locationtech.geomesa.index.index.Z3Index.currentProcessingValues match {
        case None                                           => Seq.empty[org.apache.hadoop.hbase.filter.Filter]
        case Some(Z3ProcessingValues(sfc, _, xy, _, times)) => configureZ3PushDown(sfc, xy, times)
      }

    val cqlFilter =
      if(ecql.isDefined || transform.isDefined) {
        configureCQLAndTransformPushDown(ecql, transform, sft)
      } else {
        Seq.empty[org.apache.hadoop.hbase.filter.Filter]
      }

    config.copy(hbaseFilters = config.hbaseFilters ++ z3Filter ++ cqlFilter)
  }

  private def configureCQLAndTransformPushDown(ecql: Option[Filter], transform: Option[(String, SimpleFeatureType)], sft: SimpleFeatureType) = {
    val (tform, tSchema) = transform.getOrElse(("", null))
    val tSchemaString = Option(tSchema).map(SimpleFeatureTypes.encodeType(_)).getOrElse("")
    Seq[filter.Filter](new JSimpleFeatureFilter(sft, ecql.getOrElse(Filter.INCLUDE), tform, tSchemaString))
  }

  private def configureZ3PushDown(sfc: Z3SFC, xy: Seq[(Double, Double, Double, Double)], times: Map[Short, Seq[(Long, Long)]]) = {
    // we know we're only going to scan appropriate periods, so leave out whole ones
    val wholePeriod = Seq((sfc.time.min.toLong, sfc.time.max.toLong))
    val filteredTimes = times.filter(_._2 != wholePeriod)
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

    // TODO: deal with non-points in the XZ filter
    val filt = new Z3HBaseFilter(new Z3Filter(
      normalizedXY, tOpts, minEpoch.toShort, maxEpoch.toShort, 1, 8)
    )

    Seq(filt)
  }
}

trait HBaseLikeZ3Index
    extends HBaseFeatureIndex with Z3Index[HBaseDataStore, HBaseFeature, Mutation, Query]  {
  override val version: Int = 1

}
