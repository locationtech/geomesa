/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureReader, SimpleFeatureSource}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader.HasGeoMesaFeatureReader
import org.locationtech.geomesa.index.stats.GeoMesaStats.{GeoMesaStatWriter, StatUpdater}
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedStats
import org.locationtech.geomesa.index.view.MergedQueryRunner.DataStoreQueryable
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import java.util.concurrent.CopyOnWriteArrayList

/**
  * Merged querying against multiple data stores
  *
  * @param stores delegate stores
  * @param namespace namespace
  */
class MergedDataStoreView(
    val stores: Seq[(DataStore, Option[Filter])],
    deduplicate: Boolean,
    parallel: Boolean,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb976 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 26fc359a06 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
>>>>>>> locationtech-main
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
<<<<<<< HEAD
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a0d6da1831 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
    namespace: Option[String] = None
<<<<<<< HEAD
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaStats {
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
=======
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a88 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaStats {
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a68 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaStats {
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> locationtech-main

  require(stores.nonEmpty, "No delegate stores configured")

  private[view] val runner =
    new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f }, deduplicate, parallel)

  override val stats: GeoMesaStats = new MergedStats(stores, parallel)

  override def getFeatureSource(name: Name): SimpleFeatureSource = getFeatureSource(name.getLocalPart)

  override def getFeatureSource(typeName: String): SimpleFeatureSource = {
    val sources = stores.map { case (store, filter) => (store.getFeatureSource(typeName), filter) }
    new MergedFeatureSourceView(this, sources, parallel, getSchema(typeName))
  }

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader =
    getFeatureReader(getSchema(query.getTypeName), transaction, query).reader()

  override private[geomesa] def getFeatureReader(
      sft: SimpleFeatureType,
      transaction: Transaction,
      query: Query): GeoMesaFeatureReader =
    GeoMesaFeatureReader(sft, query, runner, None)
}

object MergedDataStoreView {

  import scala.collection.JavaConverters._

  class MergedStats(stores: Seq[(DataStore, Option[Filter])], parallel: Boolean) extends GeoMesaStats {

    private val stats: Seq[(GeoMesaStats, Option[Filter])] = stores.map {
      case (s: HasGeoMesaStats, f) => (s.stats, f)
      case (s, f) => (new UnoptimizedRunnableStats(s), f)
    }

    override val writer: GeoMesaStatWriter = new MergedStatWriter(stats.map(_._1.writer))

    override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean, queryHints: Hints): Option[Long] = {
      // note: unlike most methods in this class, this will return if any of the merged stores provide a response
      def getSingle(statAndFilter: (GeoMesaStats, Option[Filter])): Option[Long] =
        statAndFilter._1.getCount(sft, mergeFilter(sft, filter, statAndFilter._2), exact, queryHints)

<<<<<<< HEAD
      if (parallel) {
        val results = new CopyOnWriteArrayList[Long]()
        stats.toList.map(s => CachedThreadPool.submit(() => getSingle(s).foreach(results.add))).foreach(_.get)
        results.asScala.reduceLeftOption(_ + _)
      } else {
        stats.flatMap(getSingle).reduceLeftOption(_ + _)
      }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
<<<<<<< HEAD
<<<<<<< HEAD
=======
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
>>>>>>> d4f1ac397 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b71311c31 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> locationtech-main
=======
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
<<<<<<< HEAD
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
      val counts = stats.flatMap { case (stat, f) => stat.getCount(sft, mergeFilter(sft, filter, f), exact, queryHints) }
      counts.reduceLeftOption(_ + _)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a7a37cdc (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
      val counts = stats.flatMap { case (stat, f) => stat.getCount(sft, mergeFilter(sft, filter, f), exact, queryHints) }
      counts.reduceLeftOption(_ + _)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 2fc500c49 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 051bc58bc (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 63a7a37cdc (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1dae86c846 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b71311c31 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> ff221938ac (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> locationtech-main
    }

    override def getMinMax[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[MinMax[T]] = {
      // note: unlike most methods in this class, this will return if any of the merged stores provide a response
      def getSingle(statAndFilter: (GeoMesaStats, Option[Filter])): Option[MinMax[T]] =
        statAndFilter._1.getMinMax[T](sft, attribute, mergeFilter(sft, filter, statAndFilter._2), exact)

<<<<<<< HEAD
      if (parallel) {
        val results = new CopyOnWriteArrayList[MinMax[T]]()
        stats.toList.map(s => CachedThreadPool.submit(() => getSingle(s).foreach(results.add))).foreach(_.get)
        results.asScala.reduceLeftOption(_ + _)
      } else {
        stats.flatMap(getSingle).reduceLeftOption(_ + _)
=======
      val bounds = stats.flatMap { case (stat, f) =>
        stat.getMinMax[T](sft, attribute, mergeFilter(sft, filter, f), exact)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
      }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4f1ac397 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
=======
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
>>>>>>> 19eba2a6c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> locationtech-main
=======
      val bounds = stats.flatMap { case (stat, f) =>
        stat.getMinMax[T](sft, attribute, mergeFilter(sft, filter, f), exact)
      }
      bounds.reduceLeftOption(_ + _)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4f1ac397 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 19eba2a6c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> locationtech-main
    }

    override def getEnumeration[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[EnumerationStat[T]] = {
      merge((stat, f) => stat.getEnumeration[T](sft, attribute, mergeFilter(sft, filter, f), exact))
    }

    override def getFrequency[T](
        sft: SimpleFeatureType,
        attribute: String,
        precision: Int,
        filter: Filter,
        exact: Boolean): Option[Frequency[T]] = {
      merge((stat, f) => stat.getFrequency[T](sft, attribute, precision, mergeFilter(sft, filter, f), exact))
    }

    override def getTopK[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[TopK[T]] = {
      merge((stat, f) => stat.getTopK[T](sft, attribute, mergeFilter(sft, filter, f), exact))
    }

    override def getHistogram[T](
        sft: SimpleFeatureType,
        attribute: String,
        bins: Int,
        min: T,
        max: T,
        filter: Filter,
        exact: Boolean): Option[Histogram[T]] = {
      merge((stat, f) => stat.getHistogram[T](sft, attribute, bins, min, max, mergeFilter(sft, filter, f), exact))
    }

    override def getZ3Histogram(
        sft: SimpleFeatureType,
        geom: String,
        dtg: String,
        period: TimePeriod,
        bins: Int,
        filter: Filter,
        exact: Boolean): Option[Z3Histogram] = {
      merge((stat, f) => stat.getZ3Histogram(sft, geom, dtg, period, bins, mergeFilter(sft, filter, f), exact))
    }

    override def getStat[T <: Stat](
        sft: SimpleFeatureType,
        query: String,
        filter: Filter,
        exact: Boolean): Option[T] = {
      merge((stat, f) => stat.getStat(sft, query, mergeFilter(sft, filter, f), exact))
    }

    override def close(): Unit = CloseWithLogging(stats.map(_._1))

    private def merge[T <: Stat](query: (GeoMesaStats, Option[Filter]) => Option[T]): Option[T] = {
      if (parallel) {
        val results = new CopyOnWriteArrayList[Option[T]]()
        stats.toList.map { case (s, f) => CachedThreadPool.submit(() => results.add(query(s, f))) }.foreach(_.get)
        results.asScala.reduceLeft((res, next) => for { r <- res; n <- next } yield { (r + n).asInstanceOf[T] })
      } else {
        // lazily evaluate each stat as we only return Some if all the child stores do
        val head = query(stats.head._1, stats.head._2)
        stats.tail.foldLeft(head) { case (result, (stat, filter)) =>
          for { r <- result; n <- query(stat, filter) } yield { (r + n).asInstanceOf[T] }
        }
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
