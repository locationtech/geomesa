/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Query, Transaction}
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.stats.GeoMesaStats.GeoMesaStatWriter
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * Runnable stats implementation, doesn't persist stat values. DataStore needs to be able to handle stat
  * query hints
  *
  * @param ds datastore
  */
class RunnableStats(ds: DataStore) extends GeoMesaStats with LazyLogging {

  override val writer: GeoMesaStatWriter = NoopStatWriter

  override def getCount(
      sft: SimpleFeatureType,
      filter: Filter,
      exact: Boolean): Option[Long] = {
    if (exact) { query[CountStat](sft, filter, Stat.Count()).map(_.count) } else { None }
  }

  override def getMinMax[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[MinMax[T]] = {
    if (exact) { query[MinMax[T]](sft, filter, Stat.MinMax(attribute)) } else { None }
  }

  override def getEnumeration[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[EnumerationStat[T]] = {
    if (exact) { query[EnumerationStat[T]](sft, filter, Stat.Enumeration(attribute)) } else { None }
  }

  override def getFrequency[T](
      sft: SimpleFeatureType,
      attribute: String,
      precision: Int,
      filter: Filter,
      exact: Boolean): Option[Frequency[T]] = {
    if (exact) { query[Frequency[T]](sft, filter, Stat.Frequency(attribute, precision)) } else { None }
  }

  override def getTopK[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[TopK[T]] = {
    if (exact) { query[TopK[T]](sft, filter, Stat.TopK(attribute)) } else { None }
  }

  override def getHistogram[T](
      sft: SimpleFeatureType,
      attribute: String,
      bins: Int,
      min: T,
      max: T,
      filter: Filter,
      exact: Boolean): Option[Histogram[T]] = {
    if (exact) {
      this.query[Histogram[T]](sft, filter, Stat.Histogram(attribute, bins, min, max)(ClassTag(min.getClass)))
    } else {
      None
    }
  }

  override def getZ3Histogram(
      sft: SimpleFeatureType,
      geom: String,
      dtg: String,
      period: TimePeriod,
      bins: Int,
      filter: Filter,
      exact: Boolean): Option[Z3Histogram] = {
    if (exact) { query(sft, filter, Stat.Z3Histogram(geom, dtg, period, bins)) } else { None }
  }

  override def getStat[T <: Stat](
      sft: SimpleFeatureType,
      query: String,
      filter: Filter,
      exact: Boolean): Option[T] = {
    if (exact) { this.query(sft, filter, query) } else { None }
  }

  override def close(): Unit = {}

  /**
    * Run an exact query against the data store
    *
    * @param sft simple feature type
    * @param filter filter
    * @param query stat string
    * @tparam T stat type
    * @return
    */
  protected def query[T <: Stat](sft: SimpleFeatureType, filter: Filter, query: String): Option[T] = {
    val q = new Query(sft.getTypeName, filter)
    q.getHints.put(QueryHints.STATS_STRING, query)
    q.getHints.put(QueryHints.ENCODE_STATS, java.lang.Boolean.TRUE)

    try {
      WithClose(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)) { reader =>
        // stats should always return exactly one result, even if there are no features in the table
        val result = if (reader.hasNext) { reader.next.getAttribute(0).asInstanceOf[String] } else {
          throw new IllegalStateException("Stats scan didn't return any rows")
        }
        Some(StatsScan.decodeStat(sft)(result).asInstanceOf[T])
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error running stats query with stats '$query' and filter '${filterToString(filter)}'", e)
        None
    }
  }
}

object RunnableStats {

  /**
    * Allows for running of stats against data stores that don't support stat queries
    *
    * @param ds datastore
    */
  class UnoptimizedRunnableStats(ds: DataStore) extends RunnableStats(ds) {
    override protected def query[T <: Stat](sft: SimpleFeatureType, filter: Filter, query: String): Option[T] = {
      try {
        val q = new Query(sft.getTypeName, filter, StatParser.propertyNames(sft, query).toArray)
        WithClose(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)) { reader =>
          val stat = Stat(reader.getFeatureType, query).asInstanceOf[T]
          CloseableIterator(reader).foreach(stat.observe)
          Some(stat)
        }
      } catch {
        case NonFatal(e) =>
          logger.error(s"Error running stats query with stats '$query' and filter '${filterToString(filter)}'", e)
          None
      }
    }
  }
}
