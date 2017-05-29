/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.fs

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset}
import java.util.Date

import com.vividsolutions.jts.geom.Point
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

case class Partition(name: String)

trait PartitionScheme {
  def getPartition(sf: SimpleFeature): Partition
  def coveringPartitions(f: Filter): Seq[String]
}

class DatePartitionScheme(fmt: DateTimeFormatter, sft: SimpleFeatureType, partitionAttribute: String)
  extends PartitionScheme {
  private val index = sft.indexOf(partitionAttribute)
  override def getPartition(sf: SimpleFeature): Partition = {
    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    Partition(fmt.format(instant))
  }

  override def coveringPartitions(f: Filter): Seq[String] = ???
}

class IntraHourPartitionScheme(minuteIntervals: Int, fmt: DateTimeFormatter, sft: SimpleFeatureType, partitionAttribute: String)
  extends PartitionScheme {
  private val index = sft.indexOf(partitionAttribute)
  override def getPartition(sf: SimpleFeature): Partition = {
    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    val adjusted = instant.withMinute(minuteIntervals*(instant.getMinute/minuteIntervals))
    Partition(fmt.format(adjusted))
  }

  override def coveringPartitions(f: Filter): Seq[String] = {
    // TODO: deal with more than just a single date range
    val interval = FilterHelper.extractIntervals(f, partitionAttribute).values
      .map { case (s,e) => (
        Instant.ofEpochMilli(s.getMillis).atZone(ZoneOffset.UTC),
        Instant.ofEpochMilli(e.getMillis).atZone(ZoneOffset.UTC)) }
    if(interval.isEmpty) Seq.empty[String]
    else {
      val (start, end) = interval.head
      start.withMinute(minuteIntervals * (start.getMinute / minuteIntervals))
      end.withMinute(minuteIntervals * (end.getMinute / minuteIntervals))
      val count = start.until(end, ChronoUnit.MINUTES) / minuteIntervals
      (0 until count.toInt).map { i => start.plusMinutes(i * minuteIntervals) }.map { i => fmt.format(i) }
    }
  }
}

class DateTimeZ2PartitionScheme(minuteIntervals: Int,
                                fmt: DateTimeFormatter,
                                bitWidth: Int,
                                sft: SimpleFeatureType,
                                dateTimeAttribute: String,
                                geomAttribute: String) extends PartitionScheme {

  private val dtgAttrIndex = sft.indexOf(dateTimeAttribute)
  private val geomAttrIndex = sft.indexOf(geomAttribute)
  private val z2 = new ZCurve2D(bitWidth/2)
  private val digits = math.round(math.log10(math.pow(bitWidth, 2))).toInt

  override def getPartition(sf: SimpleFeature): Partition = {
    val instant = sf.getAttribute(dtgAttrIndex).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    val adjusted = instant.withMinute(minuteIntervals*instant.getMinute/minuteIntervals)

    val pt = sf.getAttribute(geomAttrIndex).asInstanceOf[Point]
    val idx = z2.toIndex(pt.getX, pt.getY)
    Partition(String.format(s"${fmt.format(adjusted)}/%0${digits}d", java.lang.Long.valueOf(idx)))
  }

  override def coveringPartitions(f: Filter): Seq[String] = ???
}

class HierarchicalPartitionScheme(partitionSchemes: Seq[PartitionScheme], sep: String) extends PartitionScheme {
  override def getPartition(sf: SimpleFeature): Partition =
    Partition(partitionSchemes.map(_.getPartition(sf).name).mkString(sep))

  override def coveringPartitions(f: Filter): Seq[String] = ???
}

object PartitionScheme {

  def apply(): PartitionScheme = ???
}
