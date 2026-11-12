/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.stats.GeoMesaStats.GeoMesaStatWriter
import org.locationtech.geomesa.index.stats.impl._
import org.locationtech.geomesa.index.stats.{GeoMesaStats, NoopStatWriter, Stat}

/** Statistics sharing one resolved merge plan per operation. */
private[view] class MergedOperationStats(
    resolve: String => MergedDataStoreView,
    override val writer: GeoMesaStatWriter = NoopStatWriter,
    onClose: () => Unit = () => {}
  ) extends GeoMesaStats {

  override def getCount(
      sft: SimpleFeatureType,
      filter: Filter,
      exact: Boolean,
      queryHints: Hints): Option[Long] = run(sft, (stats, schema) => stats.getCount(schema, filter, exact, queryHints))

  override def getMinMax[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[MinMax[T]] = run(sft, (stats, schema) => stats.getMinMax[T](schema, attribute, filter, exact))

  override def getEnumeration[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[EnumerationStat[T]] =
    run(sft, (stats, schema) => stats.getEnumeration[T](schema, attribute, filter, exact))

  override def getFrequency[T](
      sft: SimpleFeatureType,
      attribute: String,
      precision: Int,
      filter: Filter,
      exact: Boolean): Option[Frequency[T]] =
    run(sft, (stats, schema) => stats.getFrequency[T](schema, attribute, precision, filter, exact))

  override def getTopK[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[TopK[T]] = run(sft, (stats, schema) => stats.getTopK[T](schema, attribute, filter, exact))

  override def getHistogram[T](
      sft: SimpleFeatureType,
      attribute: String,
      bins: Int,
      min: T,
      max: T,
      filter: Filter,
      exact: Boolean): Option[Histogram[T]] =
    run(sft, (stats, schema) => stats.getHistogram[T](schema, attribute, bins, min, max, filter, exact))

  override def getZ3Histogram(
      sft: SimpleFeatureType,
      geom: String,
      dtg: String,
      period: TimePeriod,
      bins: Int,
      filter: Filter,
      exact: Boolean): Option[Z3Histogram] =
    run(sft, (stats, schema) => stats.getZ3Histogram(schema, geom, dtg, period, bins, filter, exact))

  override def getStat[T <: Stat](
      sft: SimpleFeatureType,
      query: String,
      filter: Filter,
      exact: Boolean): Option[T] = run(sft, (stats, schema) => stats.getStat[T](schema, query, filter, exact))

  override def close(): Unit = onClose()

  private def run[T](sft: SimpleFeatureType, run: (GeoMesaStats, SimpleFeatureType) => T): T = {
    val plan = resolve(sft.getTypeName)
    run(plan.stats, plan.getSchema(sft.getTypeName))
  }
}
