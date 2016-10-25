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

  val INGEST_TABLE_NAME                      = "geomesa.ingest.table"
  val ST_FILTER_PROPERTY_NAME                = "geomesa.index.filter"
  val DEFAULT_INTERVAL_PROPERTY_NAME         = "geomesa.index.interval"
  val DEFAULT_ATTRIBUTE_NAMES                = "geomesa.index.shapefile.attribute-names"
  val DEFAULT_CACHE_SIZE_NAME                = "geomesa.index.cache-size"
  val DEFAULT_CACHE_TABLE_NAME               = "geomesa.index.cache-table"
  val DEFAULT_AGGREGATOR_CLASS_PROPERTY_NAME = "geomesa.iterators.aggregator-class"
  val DEFAULT_FILTER_PROPERTY_NAME           = "geomesa.iterators.filter-name"

  val GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE      = "geomesa.iterators.aggregator-types"
  val GEOMESA_ITERATORS_SFT_NAME                 = "geomesa.iterators.sft-name"
  val GEOMESA_ITERATORS_SFT_INDEX_VALUE          = "geomesa.iterators.sft.index-value-schema"
  val GEOMESA_ITERATORS_ATTRIBUTE_NAME           = "geomesa.iterators.attribute.name"
  val GEOMESA_ITERATORS_ATTRIBUTE_COVERED        = "geomesa.iterators.attribute.covered"
  val GEOMESA_ITERATORS_ECQL_FILTER              = "geomesa.iterators.ecql-filter"
  val GEOMESA_ITERATORS_TRANSFORM                = "geomesa.iterators.transform"
  val GEOMESA_ITERATORS_TRANSFORM_SCHEMA         = "geomesa.iterators.transform.schema"
  val GEOMESA_ITERATORS_IS_DENSITY_TYPE          = "geomesa.iterators.is-density-type"
  val GEOMESA_ITERATORS_IS_STATS_ITERATOR_TYPE   = "geomesa.iterators.is-stats-iterator-type"
  val GEOMESA_ITERATORS_INDEX_SCHEMA             = "geomesa.iterators.index.schema"
  val GEOMESA_ITERATORS_VERSION                  = "geomesa.iterators.version"

  object GeomesaSystemProperties {

    val CONFIG_FILE = getProperty(GEOMESA_CONFIG_FILE, GEOMESA_CONFIG_FILE_NAME)

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
