/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader.HasGeoMesaFeatureReader
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource.MergedSourceSft
import org.locationtech.geomesa.index.view.MergedFilterProvider.QueryContext
import org.locationtech.geomesa.index.view.MergedQueryRunner.{DataStoreQueryable, Queryable}
import org.locationtech.geomesa.utils.io.CloseQuietly

import java.util.function.UnaryOperator

/**
 * Merged querying against multiple data stores
 *
 * @param stores delegate stores
 * @param deduplicate deduplicate results across stores
 * @param parallel execute queries in parallel
 * @param namespace namespace
 */
class MergedDataStoreView(
    val stores: Seq[MergedSource[DataStore]],
    deduplicate: Boolean,
    parallel: Boolean,
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_.source), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {

  require(stores.nonEmpty, "No delegate stores configured")

  private[view] val runner =
    new MergedQueryRunner(this, stores.map(s => s.copy[Queryable](DataStoreQueryable(s.source))), deduplicate, parallel)

  override val stats: GeoMesaStats = new MergedStats(stores, parallel)

  override def getFeatureSource(name: Name): SimpleFeatureSource = getFeatureSource(name.getLocalPart)

  override def getFeatureSource(typeName: String): SimpleFeatureSource = {
    val sources = stores.map(s => s.copy(s.source.getFeatureSource(typeName)))
    new MergedFeatureSourceView(this, sources, parallel, getSchema(typeName))
  }

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader =
    getFeatureReader(getSchema(query.getTypeName), transaction, query).reader()

  override private[geomesa] def getFeatureReader(
      sft: SimpleFeatureType,
      transaction: Transaction,
      query: Query): GeoMesaFeatureReader =
    GeoMesaFeatureReader(sft, query, runner, None)

  override def dispose(): Unit = {
    try { super.dispose() } finally {
      CloseQuietly.raise(stores.map(_.provider))
    }
  }
}

object MergedDataStoreView extends LazyLogging {

  /**
   * A merged object, with associated filters
   *
   * @param source object
   * @param provider per-source filtering
   * @param decorator parallel task decorator
   * @tparam T type binding
   */
  case class MergedSource[T](
      source: T,
      provider: MergedFilterProvider = MergedFilterProvider.none,
      decorator: TaskDecorator = new TaskDecorator(None)) {

    /**
     * The filter to apply to this source, based on the current date
     *
     * @param query query
     * @param context shared query context
     * @param ev feature type loader
     * @return
     */
    def filter(query: Query, context: QueryContext)(implicit ev: MergedSourceSft[T]): Query = {
      val q = new Query(query)
      q.setFilter(filter(query.getTypeName, context))
      q
    }

    /**
     * The filter to apply to this source, based on the current date
     *
     * @param typeName feature type name
     * @param context shared query context
     * @param ev feature type loader
     * @return
     */
    def filter(typeName: String, context: QueryContext)(implicit ev: MergedSourceSft[T]): Filter = {
      val base = context.base
      provider.apply(ev.sft(source, typeName), context).fold(base) { filter =>
        if (base == Filter.INCLUDE || filter == Filter.EXCLUDE) { filter } else { FilterHelper.ff.and(base, filter) }
      }
    }
  }

  object MergedSource {

    /**
     * Helper for extracting a feature type from a merged source
     *
     * @tparam T type binding
     */
    trait MergedSourceSft[T] {
      def sft(source: T, typeName: String): SimpleFeatureType
    }

    implicit object DataSourceSft extends MergedSourceSft[DataStore] {
      override def sft(source: DataStore, typeName: String): SimpleFeatureType = source.getSchema(typeName)
    }

    implicit object FeatureSourceSft extends MergedSourceSft[SimpleFeatureSource] {
      override def sft(source: SimpleFeatureSource, typeName: String): SimpleFeatureType = {
        val sft = source.getSchema
        require(sft != null && sft.getTypeName == typeName)
        sft
      }
    }

    implicit object QueryableSft extends MergedSourceSft[Queryable] {
      override def sft(source: Queryable, typeName: String): SimpleFeatureType = source.sft(typeName)
    }
  }

  class TaskDecorator(op: Option[UnaryOperator[Runnable]]) {
    def apply(task: Runnable): Runnable = op.fold(task)(_.apply(task))
  }
}
