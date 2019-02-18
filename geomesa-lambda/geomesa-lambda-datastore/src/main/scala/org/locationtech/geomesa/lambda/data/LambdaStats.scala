/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import com.github.benmanes.caffeine.cache.LoadingCache
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.stats.GeoMesaStats.StatUpdater
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.utils.stats.{MinMax, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.reflect.ClassTag

class LambdaStats(persistent: GeoMesaStats, transients: LoadingCache[String, TransientStore]) extends GeoMesaStats {

  // note: transient stat methods always return Some

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] = {
    lazy val t = transient(sft).getCount(sft, filter, exact)
        .getOrElse(throw new IllegalStateException("Transient stats returned None"))
    persistent.getCount(sft, filter, exact) match {
      case None => None
      case Some(p) if p == -1 => Some(-1)
      case Some(p) => Some(p + t)
    }
  }

  override def getAttributeBounds[T](sft: SimpleFeatureType,
                                     attribute: String,
                                     filter: Filter,
                                     exact: Boolean): Option[MinMax[T]] = {
    val t = transient(sft).getAttributeBounds[T](sft, attribute, filter, exact)
        .getOrElse(throw new IllegalStateException("Transient stats returned None"))
    persistent.getAttributeBounds[T](sft, attribute, filter, exact).map(_ + t).orElse(Some(t))
  }

  override def getStats[T <: Stat](sft: SimpleFeatureType,
                                   attributes: Seq[String] = Seq.empty,
                                   options: Seq[Any] = Seq.empty)
                                  (implicit ct: ClassTag[T]): Seq[T] =
    persistent.getStats[T](sft, attributes, options)

  override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): Seq[T] = {
    val p = persistent.runStats[T](sft, stats, filter)
    val t = transient(sft).runStats[T](sft, stats, filter)
    p.zip(t).map(pt => pt._1 + pt._2).asInstanceOf[Seq[T]]
  }

  override def generateStats(sft: SimpleFeatureType): Seq[Stat] = persistent.generateStats(sft)

  override def statUpdater(sft: SimpleFeatureType): StatUpdater = persistent.statUpdater(sft)

  override def clearStats(sft: SimpleFeatureType): Unit = persistent.clearStats(sft)

  // note: closed by closing the wrapping stores in LambdaDataStore.dispose()
  override def close(): Unit = {}

  private def transient(sft: SimpleFeatureType): GeoMesaStats = transients.get(sft.getTypeName).stats
}
