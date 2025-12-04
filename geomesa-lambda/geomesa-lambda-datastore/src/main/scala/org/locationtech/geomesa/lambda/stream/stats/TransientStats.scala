/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.stats

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.stats.GeoMesaStats.GeoMesaStatWriter
import org.locationtech.geomesa.index.stats.impl._
import org.locationtech.geomesa.index.stats.{GeoMesaStats, NoopStatWriter, Stat}
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.reflect.ClassTag

class TransientStats(store: TransientStore) extends GeoMesaStats {

  override val writer: GeoMesaStatWriter = NoopStatWriter

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean, queryHints: Hints): Option[Long] =
    Some(CloseableIterator(store.read(Option(filter).filter(_ != Filter.INCLUDE)).iterator()).size)

  override def getMinMax[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[MinMax[T]] = getStat[MinMax[T]](sft, Stat.MinMax(attribute), filter, exact = true)

  override def getEnumeration[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[EnumerationStat[T]] = {
    if (!exact) { None } else {
      getStat[EnumerationStat[T]](sft, Stat.Enumeration(attribute), filter, exact)
    }
  }

  override def getFrequency[T](
      sft: SimpleFeatureType,
      attribute: String,
      precision: Int,
      filter: Filter,
      exact: Boolean): Option[Frequency[T]] = {
    if (!exact) { None } else {
      getStat[Frequency[T]](sft, Stat.Frequency(attribute, precision), filter, exact)
    }
  }

  override def getTopK[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[TopK[T]] = {
    if (!exact) { None } else {
      getStat[TopK[T]](sft, Stat.TopK(attribute), filter, exact)
    }
  }

  override def getHistogram[T](
      sft: SimpleFeatureType,
      attribute: String,
      bins: Int,
      min: T,
      max: T,
      filter: Filter,
      exact: Boolean): Option[Histogram[T]] = {
    if (!exact) { None } else {
      getStat[Histogram[T]](sft, Stat.Histogram(attribute, bins, min, max)(ClassTag(min.getClass)), filter, exact)
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
    if (!exact) { None } else {
      getStat[Z3Histogram](sft, Stat.Z3Histogram(geom, dtg, period, bins), filter, exact)
    }
  }

  override def getStat[T <: Stat](
      sft: SimpleFeatureType,
      query: String,
      filter: Filter,
      exact: Boolean): Option[T] = {
    if (!exact) { None } else {
      val stat = Stat(sft, query).asInstanceOf[T]
      CloseableIterator(store.read(Option(filter).filter(_ != Filter.INCLUDE)).iterator()).foreach(stat.observe)
      Some(stat)
    }
  }

  override def close(): Unit = {}
}
