/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.utils.uuid.Z3FeatureIdGenerator

import scala.collection.mutable

package object accumulo {

  // This first string is used as a SimpleFeature attribute name.
  //  Since we would like to be able to use ECQL filters, we are restricted to letters, numbers, and _'s.
  val DEFAULT_GEOMETRY_PROPERTY_NAME = "SF_PROPERTY_GEOMETRY"
  val DEFAULT_DTG_PROPERTY_NAME = "dtg"

  object GeomesaSystemProperties {

    val CONFIG_FILE = PropAndDefault("geomesa.config.file", "geomesa-site.xml")

    object QueryProperties {
      val QUERY_EXACT_COUNT    = PropAndDefault("geomesa.force.count", "false")
      val QUERY_COST_TYPE      = PropAndDefault("geomesa.query.cost.type", null)
      val QUERY_TIMEOUT_MILLIS = PropAndDefault("geomesa.query.timeout.millis", null) // default is no timeout
      // rough upper limit on the number of accumulo ranges we will generate per query
      val SCAN_RANGES_TARGET   = PropAndDefault("geomesa.scan.ranges.target", "2000")
      // if we generate more ranges than this we will split them up into sequential scans
      val SCAN_BATCH_RANGES    = PropAndDefault("geomesa.scan.ranges.batch", "20000")
    }

    object BatchWriterProperties {
      // Measured in millis, default 60 seconds
      val WRITER_LATENCY_MILLIS  = PropAndDefault("geomesa.batchwriter.latency.millis", (60 * 1000L).toString)
      // Measured in bytes, default 50mb (see Accumulo BatchWriterConfig)
      val WRITER_MEMORY_BYTES    = PropAndDefault("geomesa.batchwriter.memory", (50 * 1024 * 1024L).toString)
      val WRITER_THREADS         = PropAndDefault("geomesa.batchwriter.maxthreads", "10")
      // Timeout measured in seconds.  Likely unnecessary.
      val WRITE_TIMEOUT_MILLIS   = PropAndDefault("geomesa.batchwriter.timeout.millis", null)
    }

    object FeatureIdProperties {
      val FEATURE_ID_GENERATOR =
        PropAndDefault("geomesa.feature.id-generator", classOf[Z3FeatureIdGenerator].getCanonicalName)
    }

    object StatsProperties {
      val STAT_COMPACTION_MILLIS = PropAndDefault("geomesa.stats.compact.millis", (3600 * 1000L).toString) // one hour
    }

    case class PropAndDefault(property: String, default: String) {
      def get: String = Option(threadLocalValue.get).getOrElse(sys.props.getOrElse(property, default))
      def option: Option[String] = Option(threadLocalValue.get).orElse(sys.props.get(property)).orElse(Option(default))
      def set(value: String): Unit = sys.props.put(property, value)
      def clear(): Unit = sys.props.remove(property)

      val threadLocalValue = new ThreadLocal[String]()
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
