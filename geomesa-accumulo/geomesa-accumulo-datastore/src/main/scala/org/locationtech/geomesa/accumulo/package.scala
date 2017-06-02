/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.{GeoMesaAppendFeatureWriter, GeoMesaDataStore, GeoMesaFeatureWriter, GeoMesaModifyFeatureWriter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import scala.collection.mutable

package object accumulo {

  type AccumuloDataStoreType = GeoMesaDataStore[AccumuloDataStore, AccumuloFeature, Mutation]
  type AccumuloFeatureIndexType = GeoMesaFeatureIndex[AccumuloDataStore, AccumuloFeature, Mutation]
  type AccumuloFilterPlanType = FilterPlan[AccumuloDataStore, AccumuloFeature, Mutation]
  type AccumuloFilterStrategyType = FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation]
  type AccumuloQueryPlannerType = QueryPlanner[AccumuloDataStore, AccumuloFeature, Mutation]
  type AccumuloQueryPlanType = QueryPlan[AccumuloDataStore, AccumuloFeature, Mutation]
  type AccumuloIndexManagerType = GeoMesaIndexManager[AccumuloDataStore, AccumuloFeature, Mutation]
  type AccumuloFeatureWriterType = GeoMesaFeatureWriter[AccumuloDataStore, AccumuloFeature, Mutation, BatchWriter]
  type AccumuloAppendFeatureWriterType = GeoMesaAppendFeatureWriter[AccumuloDataStore, AccumuloFeature, Mutation, BatchWriter]
  type AccumuloModifyFeatureWriterType = GeoMesaModifyFeatureWriter[AccumuloDataStore, AccumuloFeature, Mutation, BatchWriter]

  object AccumuloProperties {

    object AccumuloQueryProperties {
      // if we generate more ranges than this we will split them up into sequential scans
      val SCAN_BATCH_RANGES    = SystemProperty("geomesa.scan.ranges.batch", "20000")
    }

    object AccumuloMapperProperties {
      val DESIRED_SPLITS_PER_TSERVER = SystemProperty("geomesa.mapreduce.splits.tserver.max")
      val DESIRED_ABSOLUTE_SPLITS = SystemProperty("geomesa.mapreduce.splits.max")
    }

    object BatchWriterProperties {
      // Measured in millis, default 60 seconds
      val WRITER_LATENCY_MILLIS  = SystemProperty("geomesa.batchwriter.latency.millis", (60 * 1000L).toString)
      // Measured in bytes, default 50mb (see Accumulo BatchWriterConfig)
      val WRITER_MEMORY_BYTES    = SystemProperty("geomesa.batchwriter.memory", (50 * 1024 * 1024L).toString)
      val WRITER_THREADS         = SystemProperty("geomesa.batchwriter.maxthreads", "10")
      // Timeout measured in seconds.  Likely unnecessary.
      val WRITE_TIMEOUT_MILLIS   = SystemProperty("geomesa.batchwriter.timeout.millis", null)
    }

    object StatsProperties {
      val STAT_COMPACTION_MILLIS = SystemProperty("geomesa.stats.compact.millis", (3600 * 1000L).toString) // one hour
    }
  }

  /**
   * Sums the values by key and returns a map containing all of the keys in the maps, with values
   * equal to the sum of all of the values for that key in the maps.
   * Sums with and aggregates the valueMaps into the aggregateInto map.
   * @param valueMaps
   * @param aggregateInto
   * @param num
   * @tparam K
   * @tparam V
   * @return the modified aggregateInto map containing the summed values
   */
  private[accumulo] def sumNumericValueMutableMaps[K, V](valueMaps: Iterable[collection.Map[K,V]],
                                                     aggregateInto: mutable.Map[K,V] = mutable.Map[K,V]())
                                                    (implicit num: Numeric[V]): mutable.Map[K, V] =
    if(valueMaps.isEmpty) aggregateInto
    else {
      valueMaps.flatten.foldLeft(aggregateInto.withDefaultValue(num.zero)) { case (mapSoFar, (k, v)) =>
        mapSoFar += ((k, num.plus(v, mapSoFar(k))))
      }
    }
}
