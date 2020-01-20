/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import com.github.benmanes.caffeine.cache.LoadingCache
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.stats.GeoMesaStats.GeoMesaStatWriter
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class LambdaStats(persistent: GeoMesaStats, transients: LoadingCache[String, TransientStore]) extends GeoMesaStats {

  // note: transient stat methods always return Some for Count and MinMax,
  // but otherwise return None if not exact

  override def writer: GeoMesaStatWriter = persistent.writer

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] = {
    persistent.getCount(sft, filter, exact).map {
      case -1L => -1L
      case p =>
        val t = transient(sft).getCount(sft, filter, exact).getOrElse {
          throw new IllegalStateException("Transient stats returned None")
        }
        p + t
    }
  }

  override def getMinMax[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[MinMax[T]] = {
    val t = transient(sft).getMinMax[T](sft, attribute, filter, exact).getOrElse {
      throw new IllegalStateException("Transient stats returned None")
    }
    persistent.getMinMax[T](sft, attribute, filter, exact).map(_ + t).orElse(Some(t))
  }

  override def getEnumeration[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[EnumerationStat[T]] = {
    val per = persistent.getEnumeration[T](sft, attribute, filter, exact)
    if (!exact) { per } else {
      per.flatMap(p => transient(sft).getEnumeration[T](sft, attribute, filter, exact).map(_ + p))
    }
  }

  override def getFrequency[T](
      sft: SimpleFeatureType,
      attribute: String,
      precision: Int,
      filter: Filter,
      exact: Boolean): Option[Frequency[T]] = {
    val per = persistent.getFrequency[T](sft, attribute, precision, filter, exact)
    if (!exact) { per } else {
      per.flatMap(p => transient(sft).getFrequency[T](sft, attribute, precision, filter, exact).map(_ + p))
    }
  }

  override def getTopK[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[TopK[T]] = {
    val per = persistent.getTopK[T](sft, attribute, filter, exact)
    if (!exact) { per } else {
      per.flatMap(p => transient(sft).getTopK[T](sft, attribute, filter, exact).map(_ + p))
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
    val per = persistent.getHistogram[T](sft, attribute, bins, min, max, filter, exact)
    if (!exact) { per } else {
      per.flatMap(p => transient(sft).getHistogram[T](sft, attribute, bins, min, max, filter, exact).map(_ + p))
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
    val per = persistent.getZ3Histogram(sft, geom, dtg, period, bins, filter, exact)
    if (!exact) { per } else {
      per.flatMap(p => transient(sft).getZ3Histogram(sft, geom, dtg, period, bins, filter, exact).map(_ + p))
    }
  }

  override def getStat[T <: Stat](
      sft: SimpleFeatureType,
      query: String,
      filter: Filter,
      exact: Boolean): Option[T] = {
    val per = persistent.getStat[T](sft, query, filter, exact)
    if (!exact) { per } else {
      per.flatMap(p => transient(sft).getStat[T](sft, query, filter, exact).map(_ + p)).asInstanceOf[Option[T]]
    }
  }

  // note: closed by closing the wrapping stores in LambdaDataStore.dispose()
  override def close(): Unit = {}

  private def transient(sft: SimpleFeatureType): GeoMesaStats = transients.get(sft.getTypeName).stats
}
