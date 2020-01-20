/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.data._
import org.geotools.data.simple.{EmptySimpleFeatureReader, SimpleFeatureSource}
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.stats.GeoMesaStats.GeoMesaStatWriter
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats, NoopStatWriter}
import org.locationtech.geomesa.index.view.RoutedDataStoreView.RoutedStats
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Routed querying against multiple data stores
  *
  * @param stores delegate stores
  * @param router query router
  * @param namespace schema namespace
  */
class RoutedDataStoreView(val stores: Seq[DataStore], val router: RouteSelector, namespace: Option[String])
    extends MergedDataStoreSchemas(stores, namespace) with HasGeoMesaStats {

  require(stores.nonEmpty, "No delegate stores configured")

  override val stats: GeoMesaStats = new RoutedStats(stores, router)

  override def getFeatureSource(name: Name): SimpleFeatureSource = getFeatureSource(name.getLocalPart)

  override def getFeatureSource(typeName: String): SimpleFeatureSource =
    new RoutedFeatureSourceView(this, getSchema(typeName))

  override def getFeatureReader(
      query: Query,
      transaction: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val sft = getSchema(query.getTypeName)
    router.route(sft, query) match {
      case None => new EmptySimpleFeatureReader(sft)
      case Some(store) => store.getFeatureReader(query, transaction)
    }
  }
}

object RoutedDataStoreView {

  class RoutedStats(stores: Seq[DataStore], router: RouteSelector) extends GeoMesaStats {

    private val delegates = stores.toStream.map {
      case gm: HasGeoMesaStats => gm -> gm.stats
      case ds => ds -> new UnoptimizedRunnableStats(ds)
    }

    override val writer: GeoMesaStatWriter = NoopStatWriter

    override def getCount(
        sft: SimpleFeatureType,
        filter: Filter,
        exact: Boolean): Option[Long] = route(sft, filter, _.getCount(sft, filter, exact))

    override def getMinMax[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[MinMax[T]] = route(sft, filter, _.getMinMax(sft, attribute, filter, exact))

    override def getEnumeration[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[EnumerationStat[T]] =
      route(sft, filter, _.getEnumeration(sft, attribute, filter, exact))

    override def getFrequency[T](
        sft: SimpleFeatureType,
        attribute: String,
        precision: Int,
        filter: Filter,
        exact: Boolean): Option[Frequency[T]] =
      route(sft, filter, _.getFrequency(sft, attribute, precision, filter, exact))

    override def getTopK[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[TopK[T]] = route(sft, filter, _.getTopK(sft, attribute, filter, exact))

    override def getHistogram[T](
        sft: SimpleFeatureType,
        attribute: String,
        bins: Int,
        min: T,
        max: T,
        filter: Filter,
        exact: Boolean): Option[Histogram[T]] =
      route(sft, filter, _.getHistogram(sft, attribute, bins, min, max, filter, exact))

    override def getZ3Histogram(
        sft: SimpleFeatureType,
        geom: String,
        dtg: String,
        period: TimePeriod,
        bins: Int,
        filter: Filter,
        exact: Boolean): Option[Z3Histogram] =
      route(sft, filter, _.getZ3Histogram(sft, geom, dtg, period, bins, filter, exact))

    override def getStat[T <: Stat](
        sft: SimpleFeatureType,
        query: String,
        filter: Filter,
        exact: Boolean): Option[T] = route(sft, filter, _.getStat(sft, query, filter, exact))

    override def close(): Unit = delegates.foreach { case (_, stat) => CloseWithLogging(stat) }

    private def route[T](sft: SimpleFeatureType, filter: Filter, fn: GeoMesaStats => Option[T]): Option[T] = {
      val ordered: Stream[GeoMesaStats] = router.route(sft, new Query(sft.getTypeName, filter)) match {
        case None => delegates.map(_._2)
        case Some(ds) =>
          val (head, tail) = delegates.partition(_._1.eq(ds))
          (head ++ tail).map(_._2)
      }
      ordered.flatMap(fn.apply).headOption
    }
  }
}
