/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa

import scala.collection.mutable

package object accumulo {

  // This first string is used as a SimpleFeature attribute name.
  //  Since we would like to be able to use ECQL filters, we are restricted to letters, numbers, and _'s.
  val DEFAULT_GEOMETRY_PROPERTY_NAME = "SF_PROPERTY_GEOMETRY"
  val DEFAULT_DTG_PROPERTY_NAME = "dtg"
  val DEFAULT_DTG_END_PROPERTY_NAME = "dtg_end_time"

  val DEFAULT_FEATURE_TYPE = "geomesa.feature.type"
  val DEFAULT_SCHEMA_NAME  = "geomesa.index.schema"
  val DEFAULT_FEATURE_NAME = "geomesa.index.feature"

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
  val GEOMESA_ITERATORS_IS_TEMPORAL_DENSITY_TYPE = "geomesa.iterators.is-temporal-density-type"
  val GEOMESA_ITERATORS_INDEX_SCHEMA             = "geomesa.iterators.index.schema"
  val GEOMESA_ITERATORS_VERSION                  = "geomesa.iterators.version"

  object GeomesaSystemProperties {

    val CONFIG_FILE = PropAndDefault("geomesa.config.file", "geomesa-site.xml")

    object QueryProperties {
      val QUERY_EXACT_COUNT    = PropAndDefault("geomesa.force.count", "false")
      val QUERY_TIMEOUT_MILLIS = PropAndDefault("geomesa.query.timeout.millis", "300000") // default 5 minutes
    }

    object BatchWriterProperties {
      // Measured in millis, default 10 seconds
      val WRITER_LATENCY_MILLIS  = PropAndDefault("geomesa.batchwriter.latency.millis", "10000")
      // Measured in bytes, default 1 megabyte
      val WRITER_MEMORY_BYTES    = PropAndDefault("geomesa.batchwriter.memory", "1000000")
      val WRITER_THREADS         = PropAndDefault("geomesa.batchwriter.maxthreads", "10")
      // Timeout measured in seconds.  Likely unnecessary.
      val WRITE_TIMEOUT_MILLIS   = PropAndDefault("geomesa.batchwriter.timeout.millis", null)
    }

    case class PropAndDefault(property: String, default: String) {
      def get: String = sys.props.getOrElse(property, default)
      def set(value: String): Unit = sys.props.put(property, value)
      def clear(): Unit = sys.props.remove(property)
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
