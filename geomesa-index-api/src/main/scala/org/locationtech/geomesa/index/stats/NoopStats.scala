/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import org.locationtech.geomesa.index.stats.GeoMesaStats.StatUpdater
import org.locationtech.geomesa.utils.stats.{MinMax, Stat}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.reflect.ClassTag

object NoopStats extends GeoMesaStats {

  override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] = None

  override def getAttributeBounds[T](sft: SimpleFeatureType,
                                     attribute: String,
                                     filter: Filter,
                                     exact: Boolean): Option[MinMax[T]] = None

  override def getStats[T <: Stat](sft: SimpleFeatureType,
                                   attributes: Seq[String],
                                   options: Seq[Any])(implicit ct: ClassTag[T]): Seq[T] = Seq.empty

  override def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter): Seq[T] = Seq.empty

  override def generateStats(sft: SimpleFeatureType): Seq[Stat] = Seq.empty

  override def statUpdater(sft: SimpleFeatureType): StatUpdater = NoopStatUpdater

  override def clearStats(sft: SimpleFeatureType): Unit = {}

  override def close(): Unit = {}

  object NoopStatUpdater extends StatUpdater {
    override def add(sf: SimpleFeature): Unit = {}
    override def remove(sf: SimpleFeature): Unit = {}
    override def flush(): Unit = {}
    override def close(): Unit = {}
  }
}
