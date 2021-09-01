/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
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
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ac2d5a925f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> bcf168a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
    namespace: Option[String] = None
<<<<<<< HEAD
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaStats {
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
=======
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 84634d919 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 3c4fc5a828 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
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
<<<<<<< HEAD
>>>>>>> 56f97cd271 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ac2d5a925 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 11addb1ce7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 42bfd74c63 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> c6103aab4 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
<<<<<<< HEAD
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4c9fb0f6e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> b51d3046a2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 425a920af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6eb7e598f3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a47c538b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> a0d6da1831 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 6abf5007af (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
    namespace: Option[String] = None
<<<<<<< HEAD
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaStats {
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91b0d6a888 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaStats {
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
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
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 0f0dd6560e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaStats {
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {
=======
=======
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_._1), namespace) with HasGeoMesaStats {
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))

  require(stores.nonEmpty, "No delegate stores configured")

  private[view] val runner =
<<<<<<< HEAD
    new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f }, deduplicate, parallel)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
    new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f }, deduplicate)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
    new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f }, deduplicate)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
    new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f }, deduplicate)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
    new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f }, deduplicate)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 8effb11c46 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
    new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f }, deduplicate)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 91ead0a832 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
    new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f }, deduplicate)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> d36d85cd8e (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
    new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f }, deduplicate)
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 38876e069f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
    new MergedQueryRunner(this, stores.map { case (ds, f) => DataStoreQueryable(ds) -> f }, deduplicate)
<<<<<<< HEAD
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 1b25b28b73 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 51ab350ee2 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e17f495391 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))

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
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
<<<<<<< HEAD
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
      val counts = stats.flatMap { case (stat, f) => stat.getCount(sft, mergeFilter(sft, filter, f), exact, queryHints) }
      counts.reduceLeftOption(_ + _)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
<<<<<<< HEAD
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
<<<<<<< HEAD
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a7a37cdc (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
      val counts = stats.flatMap { case (stat, f) => stat.getCount(sft, mergeFilter(sft, filter, f), exact, queryHints) }
      counts.reduceLeftOption(_ + _)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 2fc500c49 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 051bc58bc (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 63a7a37cdc (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1dae86c846 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b71311c31 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> ff221938ac (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val counts = stats.flatMap { case (stat, f) => stat.getCount(sft, mergeFilter(sft, filter, f), exact, queryHints) }
      counts.reduceLeftOption(_ + _)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
=======
      val counts = stats.flatMap { case (stat, f) => stat.getCount(sft, mergeFilter(sft, filter, f), exact, queryHints) }
      counts.reduceLeftOption(_ + _)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
    }

    override def getMinMax[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[MinMax[T]] = {
      // note: unlike most methods in this class, this will return if any of the merged stores provide a response
<<<<<<< HEAD
      def getSingle(statAndFilter: (GeoMesaStats, Option[Filter])): Option[MinMax[T]] =
        statAndFilter._1.getMinMax[T](sft, attribute, mergeFilter(sft, filter, statAndFilter._2), exact)

<<<<<<< HEAD
      if (parallel) {
        val results = new CopyOnWriteArrayList[MinMax[T]]()
        stats.toList.map(s => CachedThreadPool.submit(() => getSingle(s).foreach(results.add))).foreach(_.get)
        results.asScala.reduceLeftOption(_ + _)
      } else {
        stats.flatMap(getSingle).reduceLeftOption(_ + _)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      val bounds = stats.flatMap { case (stat, f) =>
        stat.getMinMax[T](sft, attribute, mergeFilter(sft, filter, f), exact)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
      val bounds = stats.flatMap { case (stat, f) =>
        stat.getMinMax[T](sft, attribute, mergeFilter(sft, filter, f), exact)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val bounds = stats.flatMap { case (stat, f) =>
        stat.getMinMax[T](sft, attribute, mergeFilter(sft, filter, f), exact)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
      }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
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
>>>>>>> d4f1ac3977 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 19eba2a6c8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4f1ac397 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7221b07f1c (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
=======
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
>>>>>>> 81fe057376 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val seq = if (parallel) { stats.par } else { stats }
      seq.flatMap(getSingle).reduceLeftOption(_ + _)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
      val bounds = stats.flatMap { case (stat, f) =>
        stat.getMinMax[T](sft, attribute, mergeFilter(sft, filter, f), exact)
      }
      bounds.reduceLeftOption(_ + _)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d4f1ac3977 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
      }
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
      }
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 19eba2a6c8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0733de3db3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4f1ac397 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 7221b07f1c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 19eba2a6c (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 0733de3db3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eea6a40fa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 81fe057376 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
