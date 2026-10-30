/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data.DataStore
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.stats.GeoMesaStats.{GeoMesaStatWriter, StatUpdater}
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.impl._
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats, Stat}
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource.MergedSourceSft
import org.locationtech.geomesa.index.view.MergedDataStoreView.{MergedSource, TaskDecorator}
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Merges stats
 *
 * @param stores data stores
 * @param parallel parallel execution or serial
 * @param decorator task decorator for parallel tasks
 */
class MergedStats(stores: Seq[MergedSource[DataStore]], parallel: Boolean, decorator: TaskDecorator)
  extends GeoMesaStats {

  import org.locationtech.geomesa.index.view.MergedStats.{MergedStatWriter, StatsWithSchemas}
  import org.locationtech.geomesa.index.view.MergedStats.StatsWithSchemas.StatsWithSchemasSft

  import scala.collection.JavaConverters._

  // noinspection ScalaUnusedSymbol
  private implicit val ev: MergedSourceSft[StatsWithSchemas] = StatsWithSchemasSft

  private val stats: Seq[MergedSource[StatsWithSchemas]] = stores.map {
    case s@MergedSource(stats: HasGeoMesaStats, _, _, _) => s.copy(StatsWithSchemas(stats.stats, s.source))
    case s => s.copy(StatsWithSchemas(new UnoptimizedRunnableStats(s.source), s.source))
  }

  override val writer: GeoMesaStatWriter = new MergedStatWriter(stats.map(_.source.stats.writer))

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean, queryHints: Hints): Option[Long] = {
    val now = Instant.now() // shared current time passed to all stores for consistency in bounds calculations
    // note: unlike most methods in this class, this will return if any of the merged stores provide a response
    def getSingle(source: MergedSource[StatsWithSchemas]): Option[Long] = {
      val schema = source.source.ds.getSchema(sft.getTypeName)
      source.source.stats.getCount(schema, source.filter(sft.getTypeName, filter, now), exact, queryHints)
    }

    if (parallel) {
      val results = new CopyOnWriteArrayList[Long]()
      stats.toList.map(s => CachedThreadPool.submit(decorator(() => getSingle(s).foreach(results.add)))).foreach(_.get)
      results.asScala.reduceLeftOption(_ + _)
    } else {
      stats.flatMap(getSingle).reduceLeftOption(_ + _)
    }
  }

  override def getMinMax[T](
    sft: SimpleFeatureType,
    attribute: String,
    filter: Filter,
    exact: Boolean): Option[MinMax[T]] = {
    val now = Instant.now() // shared current time passed to all stores for consistency in bounds calculations
    // note: unlike most methods in this class, this will return if any of the merged stores provide a response
    def getSingle(source: MergedSource[StatsWithSchemas]): Option[MinMax[T]] = {
      val schema = source.source.ds.getSchema(sft.getTypeName)
      source.source.stats.getMinMax[T](schema, attribute, source.filter(sft.getTypeName, filter, now), exact)
    }

    if (parallel) {
      val results = new CopyOnWriteArrayList[MinMax[T]]()
      stats.toList.map(s => CachedThreadPool.submit(decorator(() => getSingle(s).foreach(results.add)))).foreach(_.get)
      results.asScala.reduceLeftOption(_ + _)
    } else {
      stats.flatMap(getSingle).reduceLeftOption(_ + _)
    }
  }

  override def getEnumeration[T](
    sft: SimpleFeatureType,
    attribute: String,
    filter: Filter,
    exact: Boolean): Option[EnumerationStat[T]] = {
    merge(sft.getTypeName, filter, (stat, f) => stat.getEnumeration[T](sft, attribute, f, exact))
  }

  override def getFrequency[T](
    sft: SimpleFeatureType,
    attribute: String,
    precision: Int,
    filter: Filter,
    exact: Boolean): Option[Frequency[T]] = {
    merge(sft.getTypeName, filter, (stat, f) => stat.getFrequency[T](sft, attribute, precision, f, exact))
  }

  override def getTopK[T](
    sft: SimpleFeatureType,
    attribute: String,
    filter: Filter,
    exact: Boolean): Option[TopK[T]] = {
    merge(sft.getTypeName, filter, (stat, f) => stat.getTopK[T](sft, attribute, f, exact))
  }

  override def getHistogram[T](
    sft: SimpleFeatureType,
    attribute: String,
    bins: Int,
    min: T,
    max: T,
    filter: Filter,
    exact: Boolean): Option[Histogram[T]] = {
    merge(sft.getTypeName, filter, (stat, f) => stat.getHistogram[T](sft, attribute, bins, min, max, f, exact))
  }

  override def getZ3Histogram(
    sft: SimpleFeatureType,
    geom: String,
    dtg: String,
    period: TimePeriod,
    bins: Int,
    filter: Filter,
    exact: Boolean): Option[Z3Histogram] = {
    merge(sft.getTypeName, filter, (stat, f) => stat.getZ3Histogram(sft, geom, dtg, period, bins, f, exact))
  }

  override def getStat[T <: Stat](
    sft: SimpleFeatureType,
    query: String,
    filter: Filter,
    exact: Boolean): Option[T] = {
    merge(sft.getTypeName, filter, (stat, f) => stat.getStat(sft, query, f, exact))
  }

  override def close(): Unit = CloseWithLogging(stats.map(_.source.stats))

  private def merge[T <: Stat](typeName: String, filter: Filter, query: (GeoMesaStats, Filter) => Option[T]): Option[T] = {
    val now = Instant.now()
    if (parallel) {
      val results = new CopyOnWriteArrayList[Option[T]]()
      stats.toList.map(s => CachedThreadPool.submit(decorator(() => results.add(query(s.source.stats, s.filter(typeName, filter, now)))))).foreach(_.get)
      results.asScala.reduceLeft((res, next) => for { r <- res; n <- next } yield { (r + n).asInstanceOf[T] })
    } else {
      // lazily evaluate each stat as we only return Some if all the child stores do
      val head = query(stats.head.source.stats, stats.head.filter(typeName, filter, now))
      stats.tail.foldLeft(head) { case (result, stat) =>
        for {
          r <- result
          n <- query(stat.source.stats, stat.filter(typeName, filter, now))
        } yield {
          (r + n).asInstanceOf[T]
        }
      }
    }
  }
}

object MergedStats {

  private case class StatsWithSchemas(stats: GeoMesaStats, ds: DataStore)

  private object StatsWithSchemas {
    implicit object StatsWithSchemasSft extends MergedSourceSft[StatsWithSchemas] {
      override def sft(source: StatsWithSchemas, typeName: String): SimpleFeatureType = source.ds.getSchema(typeName)
    }
  }

  private class MergedStatWriter(writers: Seq[GeoMesaStatWriter]) extends GeoMesaStatWriter {
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

  private class MergedStatUpdater(updaters: Seq[StatUpdater]) extends StatUpdater {
    override def add(sf: SimpleFeature): Unit = updaters.foreach(_.add(sf))
    override def remove(sf: SimpleFeature): Unit = updaters.foreach(_.remove(sf))
    override def flush(): Unit = updaters.foreach(_.flush())
    override def close(): Unit = CloseWithLogging(updaters)
  }
}
