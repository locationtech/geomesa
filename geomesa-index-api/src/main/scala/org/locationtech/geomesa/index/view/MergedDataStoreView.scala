/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureReader, SimpleFeatureSource}
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader
import org.locationtech.geomesa.index.stats.GeoMesaStats.{GeoMesaStatWriter, StatUpdater}
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedStats
import org.locationtech.geomesa.index.view.MergedQueryRunner.DataStoreQueryable
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Merged querying against multiple data stores
  *
  * @param stores delegate stores
  * @param namespace namespace
  */
class MergedDataStoreView(val stores: Seq[(DataStore, Option[Filter])], namespace: Option[String] = None)
    extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaStats {

  require(stores.nonEmpty, "No delegate stores configured")

  private [view] val runner = new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f })

  override val stats: GeoMesaStats = new MergedStats(stores)

  override def getFeatureSource(name: Name): SimpleFeatureSource = getFeatureSource(name.getLocalPart)

  override def getFeatureSource(typeName: String): SimpleFeatureSource = {
    val sources = stores.map { case (store, filter) => (store.getFeatureSource(typeName), filter) }
    new MergedFeatureSourceView(this, sources, getSchema(typeName))
  }

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader =
    GeoMesaFeatureReader(getSchema(query.getTypeName), query, runner, None, None)
}

object MergedDataStoreView {

  class MergedStats(stores: Seq[(DataStore, Option[Filter])]) extends GeoMesaStats {

    private val stats = stores.map {
      case (s: HasGeoMesaStats, f) => (s.stats, f)
      case (s, f) => (new UnoptimizedRunnableStats(s), f)
    }

    override val writer: GeoMesaStatWriter = new MergedStatWriter(stats.map(_._1.writer))

    override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] = {
      // note: unlike most methods in this class, this will return if any of the merged stores provide a response
      val counts = stats.flatMap { case (stat, f) => stat.getCount(sft, mergeFilter(filter, f), exact) }
      counts.reduceLeftOption(_ + _)
    }

    override def getMinMax[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[MinMax[T]] = {
      // note: unlike most methods in this class, this will return if any of the merged stores provide a response
      val bounds = stats.flatMap { case (stat, f) =>
        stat.getMinMax[T](sft, attribute, mergeFilter(filter, f), exact)
      }
      bounds.reduceLeftOption(_ + _)
    }

    override def getEnumeration[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[EnumerationStat[T]] = {
      merge((stat, f) => stat.getEnumeration[T](sft, attribute, mergeFilter(filter, f), exact))
    }

    override def getFrequency[T](
        sft: SimpleFeatureType,
        attribute: String,
        precision: Int,
        filter: Filter,
        exact: Boolean): Option[Frequency[T]] = {
      merge((stat, f) => stat.getFrequency[T](sft, attribute, precision, mergeFilter(filter, f), exact))
    }

    override def getTopK[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[TopK[T]] = {
      merge((stat, f) => stat.getTopK[T](sft, attribute, mergeFilter(filter, f), exact))
    }

    override def getHistogram[T](
        sft: SimpleFeatureType,
        attribute: String,
        bins: Int,
        min: T,
        max: T,
        filter: Filter,
        exact: Boolean): Option[Histogram[T]] = {
      merge((stat, f) => stat.getHistogram[T](sft, attribute, bins, min, max, mergeFilter(filter, f), exact))
    }

    override def getZ3Histogram(
        sft: SimpleFeatureType,
        geom: String,
        dtg: String,
        period: TimePeriod,
        bins: Int,
        filter: Filter,
        exact: Boolean): Option[Z3Histogram] = {
      merge((stat, f) => stat.getZ3Histogram(sft, geom, dtg, period, bins, mergeFilter(filter, f), exact))
    }

    override def getStat[T <: Stat](
        sft: SimpleFeatureType,
        query: String,
        filter: Filter,
        exact: Boolean): Option[T] = {
      merge((stat, f) => stat.getStat(sft, query, mergeFilter(filter, f), exact))
    }

    override def close(): Unit = CloseWithLogging(stats.map(_._1))

    private def merge[T <: Stat](query: (GeoMesaStats, Option[Filter]) => Option[T]): Option[T] = {
      // lazily evaluate each stat as we only return Some if all the child stores do
      val head = query(stats.head._1, stats.head._2)
      stats.tail.foldLeft(head) { case (result, (stat, filter)) =>
        for { r <- result; n <- query(stat, filter) } yield { (r + n).asInstanceOf[T] }
      }
    }
  }

  class MergedStatWriter(writers: Seq[GeoMesaStatWriter]) extends GeoMesaStatWriter {
    override def analyze(sft: SimpleFeatureType): Seq[Stat] = {
      writers.map(_.analyze(sft)).reduceLeft[Seq[Stat]] { case (left, right) =>
        left.zip(right).map { case (l, r) => l + r }
      }
    }

    override def updater(sft: SimpleFeatureType): StatUpdater = new MergedStatUpdater(writers.map(_.updater(sft)))

    override def rename(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit =
      writers.foreach(_.rename(sft, previous))

    override def clear(sft: SimpleFeatureType): Unit = writers.foreach(_.clear(sft))
  }

  class MergedStatUpdater(updaters: Seq[StatUpdater]) extends StatUpdater {
    override def add(sf: SimpleFeature): Unit = updaters.foreach(_.add(sf))
    override def remove(sf: SimpleFeature): Unit = updaters.foreach(_.remove(sf))
    override def flush(): Unit = updaters.foreach(_.flush())
    override def close(): Unit = CloseWithLogging(updaters)
  }
}
