/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.utils.conf.GeoMesaProperties._

import scala.collection.mutable

package object accumulo {

  // This first string is used as a SimpleFeature attribute name.
  //  Since we would like to be able to use ECQL filters, we are restricted to letters, numbers, and _'s.
  val DEFAULT_GEOMETRY_PROPERTY_NAME = "SF_PROPERTY_GEOMETRY"
  val DEFAULT_DTG_PROPERTY_NAME = "dtg"

  object GeomesaSystemProperties {

    val CONFIG_FILE = getProperty(GEOMESA_CONFIG_FILE_PROP, GEOMESA_CONFIG_FILE_NAME)

    object QueryProperties {
      val QUERY_EXACT_COUNT    = GEOMESA_FORCE_COUNT
      val QUERY_COST_TYPE      = GEOMESA_QUERY_COST_TYPE
      val QUERY_TIMEOUT_MILLIS = GEOMESA_QUERY_TIMEOUT_MILLS
      val SCAN_RANGES_TARGET   = GEOMESA_SCAN_RANGES_TARGET
      val SCAN_BATCH_RANGES    = GEOMESA_SCAN_RANGES_BATCH
    }

    object BatchWriterProperties {
      val WRITER_LATENCY_MILLIS  = GEOMESA_BATCHWRITER_LATENCY_MILLS
      val WRITER_MEMORY_BYTES    = GEOMESA_BATCHWRITER_MEMORY
      val WRITER_THREADS         = GEOMESA_BATCHWRITER_MAXTHREADS
      val WRITE_TIMEOUT_MILLIS   = GEOMESA_BATCHWRITER_TIMEOUT_MILLS
    }

    object FeatureIdProperties {
      val FEATURE_ID_GENERATOR = GEOMESA_FEATURE_ID_GENERATOR
    }

    object StatsProperties {
      val STAT_COMPACTION_MILLIS = GEOMESA_STATS_COMPACT_MILLIS
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
