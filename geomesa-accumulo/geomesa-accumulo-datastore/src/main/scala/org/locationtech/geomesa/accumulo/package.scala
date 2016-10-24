/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa

import java.util.Map.Entry

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.{GeoMesaAppendFeatureWriter, GeoMesaDataStore, GeoMesaFeatureWriter, GeoMesaModifyFeatureWriter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import scala.collection.mutable

package object accumulo {

  type AccumuloDataStoreType = GeoMesaDataStore[AccumuloDataStore, AccumuloFeature, Seq[Mutation], Entry[Key, Value]]
  type AccumuloFeatureIndexType = GeoMesaFeatureIndex[AccumuloDataStore, AccumuloFeature, Seq[Mutation], Entry[Key, Value]]
  type AccumuloFilterPlanType = FilterPlan[AccumuloDataStore, AccumuloFeature, Seq[Mutation], Entry[Key, Value]]
  type AccumuloFilterStrategyType = FilterStrategy[AccumuloDataStore, AccumuloFeature, Seq[Mutation], Entry[Key, Value]]
  type AccumuloQueryPlannerType = QueryPlanner[AccumuloDataStore, AccumuloFeature, Seq[Mutation], Entry[Key, Value]]
  type AccumuloQueryPlanType = QueryPlan[AccumuloDataStore, AccumuloFeature, Seq[Mutation], Entry[Key, Value]]
  type AccumuloIndexManagerType = GeoMesaIndexManager[AccumuloDataStore, AccumuloFeature, Seq[Mutation], Entry[Key, Value]]
  type AccumuloFeatureWriterType = GeoMesaFeatureWriter[AccumuloDataStore, AccumuloFeature, Seq[Mutation], Entry[Key, Value], BatchWriter]
  type AccumuloAppendFeatureWriterType = GeoMesaAppendFeatureWriter[AccumuloDataStore, AccumuloFeature, Seq[Mutation], Entry[Key, Value], BatchWriter]
  type AccumuloModifyFeatureWriterType = GeoMesaModifyFeatureWriter[AccumuloDataStore, AccumuloFeature, Seq[Mutation], Entry[Key, Value], BatchWriter]

  // This first string is used as a SimpleFeature attribute name.
  //  Since we would like to be able to use ECQL filters, we are restricted to letters, numbers, and _'s.
  val DEFAULT_GEOMETRY_PROPERTY_NAME = "SF_PROPERTY_GEOMETRY"
  val DEFAULT_DTG_PROPERTY_NAME = "dtg"

  object AccumuloProperties {

    val CONFIG_FILE = SystemProperty("geomesa.config.file", "geomesa-site.xml")

    object AccumuloQueryProperties {
      // if we generate more ranges than this we will split them up into sequential scans
      val SCAN_BATCH_RANGES    = SystemProperty("geomesa.scan.ranges.batch", "20000")
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
