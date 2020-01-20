/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.stats.GeoMesaStats.{GeoMesaStatWriter, StatUpdater}
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

package object stats {

  /**
    * Marker trait for classes with stats
    */
  trait HasGeoMesaStats {
    def stats: GeoMesaStats
  }

  /**
    * Stats implementation that will always return None
    */
  object NoopStats extends GeoMesaStats {

    override val writer: GeoMesaStatWriter = NoopStatWriter

    override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean): Option[Long] = None

    override def getMinMax[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[MinMax[T]] = None

    override def getEnumeration[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[EnumerationStat[T]] = None

    override def getFrequency[T](
        sft: SimpleFeatureType,
        attribute: String,
        precision: Int,
        filter: Filter,
        exact: Boolean): Option[Frequency[T]] = None

    override def getTopK[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[TopK[T]] = None

    override def getHistogram[T](
        sft: SimpleFeatureType,
        attribute: String,
        bins: Int,
        min: T,
        max: T,
        filter: Filter,
        exact: Boolean): Option[Histogram[T]] = None

    override def getZ3Histogram(
        sft: SimpleFeatureType,
        geom: String,
        dtg: String,
        period: TimePeriod,
        bins: Int,
        filter: Filter,
        exact: Boolean): Option[Z3Histogram] = None

    override def getStat[T <: Stat](
        sft: SimpleFeatureType,
        query: String,
        filter: Filter,
        exact: Boolean): Option[T] = None

    override def close(): Unit = {}
  }

  object NoopStatWriter extends GeoMesaStatWriter {
    override def analyze(sft: SimpleFeatureType): Seq[Stat] = Seq.empty
    override def updater(sft: SimpleFeatureType): StatUpdater = NoopStatUpdater
    override def clear(sft: SimpleFeatureType): Unit = {}
    override def rename(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {}
  }

  object NoopStatUpdater extends StatUpdater {
    override def add(sf: SimpleFeature): Unit = {}
    override def remove(sf: SimpleFeature): Unit = {}
    override def flush(): Unit = {}
    override def close(): Unit = {}
  }
}
