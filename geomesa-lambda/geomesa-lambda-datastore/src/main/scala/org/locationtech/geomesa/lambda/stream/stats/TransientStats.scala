/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.stats

import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.stats.GeoMesaStats.StatUpdater
import org.locationtech.geomesa.index.stats.NoopStats.NoopStatUpdater
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.utils.stats.{MinMax, SeqStat, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.reflect.ClassTag

class TransientStats(store: TransientStore) extends GeoMesaStats {

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] =
    Some(store.read(Option(filter)).length)

  override def getAttributeBounds[T](sft: SimpleFeatureType,
                                     attribute: String,
                                     filter: Filter,
                                     exact: Boolean): Option[MinMax[T]] = {
    val stat = Stat(store.sft, Stat.MinMax(attribute)).asInstanceOf[MinMax[T]]
    store.read(Option(filter)).foreach(stat.observe)
    Some(stat)
  }

  override def getStats[T <: Stat](sft: SimpleFeatureType,
                                   attributes: Seq[String] = Seq.empty,
                                   options: Seq[Any] = Seq.empty)
                                  (implicit ct: ClassTag[T]): Seq[T] = Seq.empty

  override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): Seq[T] = {
    val stat = Stat(store.sft, stats)
    store.read(Option(filter)).foreach(stat.observe)
    stat match {
      case s: SeqStat => s.stats.asInstanceOf[Seq[T]]
      case s          => Seq(s).asInstanceOf[Seq[T]]
    }
  }

  override def generateStats(sft: SimpleFeatureType): Seq[Stat] = Seq.empty

  override def statUpdater(sft: SimpleFeatureType): StatUpdater = NoopStatUpdater

  override def clearStats(sft: SimpleFeatureType): Unit = {}

  override def close(): Unit = {}
}
