/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.partition

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.filter.{Bounds, FilterHelper, FilterValues}
import org.locationtech.geomesa.index.conf.partition.TimePartition.CustomPartitionCache
import org.locationtech.geomesa.index.metadata.{CachedLazyMetadata, GeoMesaMetadata, HasGeoMesaMetadata}
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.locationtech.geomesa.utils.text.DateParsing
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter


/**
  * Partition by time period. Partitions consist of 0-padded 5-digit strings
  *
  * @param metadata used for registering outside tables for query
  * @param typeName simple feature type name
  * @param dtg date attribute
  * @param dtgIndex date attribute index
  * @param period time period
  */
class TimePartition(metadata: GeoMesaMetadata[String], typeName: String, dtg: String, dtgIndex: Int, period: TimePeriod)
    extends TablePartition {

  import TimePartition.PartitionKeyPrefix

  private val toBin = BinnedTime.dateToBin(period)
  private val toDate = BinnedTime.binnedTimeToDate(period)

  private val cache = new CustomPartitionCache(metadata, typeName)

  /**
    * Register a new partition
    *
    * @param partition partition name
    * @param start start time for the partition
    * @param end end time for the partition
    */
  def register(partition: String, start: ZonedDateTime, end: ZonedDateTime): Unit = {
    val dates = s"${DateParsing.format(start)}/${DateParsing.format(end)}"
    metadata.insert(typeName, s"$PartitionKeyPrefix$partition", dates)
    cache.invalidate()
  }

  override def partition(feature: SimpleFeature): String = {
    val date = feature.getAttribute(dtgIndex).asInstanceOf[Date]
    f"${toBin(ZonedDateTime.ofInstant(toInstant(date), ZoneOffset.UTC))}%05d" // a short should fit into 5 digits
  }

  override def partitions(filter: Filter): Option[Seq[String]] = {
    val intervals = FilterHelper.extractIntervals(filter, dtg)
    if (intervals.disjoint) {
      Some(Seq.empty)
    } else if (intervals.isEmpty || !intervals.forall(_.isBoundedBothSides)) {
      None
    } else {
      val bins = intervals.values.flatMap(i => Range.inclusive(toBin(i.lower.value.get), toBin(i.upper.value.get)))
      val names = bins.distinct.map(b => f"$b%05d") ++ cache.customPartitions().collect {
        case (p, start, end) if overlaps(intervals, start, end) => p
      }
      Some(names)
    }
  }

  override def recover(partition: String): AnyRef =
    new Date(toDate(BinnedTime(partition.toShort, 0L)).toInstant.toEpochMilli)

  private def overlaps(intervals: FilterValues[Bounds[ZonedDateTime]], start: ZonedDateTime, end: ZonedDateTime): Boolean =
    intervals.exists(i => i.lower.value.forall(_.isBefore(end)) && i.upper.value.forall(_.isAfter(start)))
}

object TimePartition {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val Name = "time"

  private val PartitionKeyPrefix = "partition."

  private val cache = Caffeine.newBuilder().build(
    new CacheLoader[(GeoMesaMetadata[String], String, String, Int, TimePeriod), TablePartition]() {
      override def load(key: (GeoMesaMetadata[String], String, String, Int, TimePeriod)): TablePartition =
        new TimePartition(key._1, key._2, key._3, key._4, key._5)
    }
  )

  class TimePartitionFactory extends TablePartitionFactory {

    override def name: String = Name

    override def create(ds: HasGeoMesaMetadata[String], sft: SimpleFeatureType): TablePartition = {
      val dtg = sft.getDtgField.getOrElse {
        throw new IllegalArgumentException("Can't use time partitioning without a date field")
      }
      cache.get((ds.metadata, sft.getTypeName, dtg, sft.getDtgIndex.get, sft.getZ3Interval))
    }
  }

  /**
    * Caches parsed partition bounds
    *
    * @param metadata metadata for reading bounds
    * @param typeName simple feature type name
    */
  private class CustomPartitionCache(metadata: GeoMesaMetadata[String], typeName: String) {

    private val expiry = CachedLazyMetadata.Expiry.toDuration.get.toMillis

    private var partitions: Seq[(String, ZonedDateTime, ZonedDateTime)] = _
    private var reload = 0L

    def customPartitions(): Seq[(String, ZonedDateTime, ZonedDateTime)] = {
      if (System.currentTimeMillis() < reload) { partitions } else {
        reload = System.currentTimeMillis() + expiry
        partitions = metadata.scan(typeName, PartitionKeyPrefix).map { case (k, v) =>
          val Array(start, end) = v.split("/").map(DateParsing.parse(_))
          (k.substring(PartitionKeyPrefix.length), start, end)
        }
        partitions
      }
    }

    def invalidate(): Unit = reload = 0L
  }
}
