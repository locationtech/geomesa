/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.fs

import java.text.NumberFormat
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Date

import com.google.common.primitives.Longs
import com.vividsolutions.jts.geom.Point
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case class Partition(name: String)

trait PartitionScheme {
  def getPartition(sf: SimpleFeature): Partition
}

class DatePartitionScheme(fmt: DateTimeFormatter, sft: SimpleFeatureType, partitionAttribute: String)
  extends PartitionScheme {
  private val index = sft.indexOf(partitionAttribute)
  override def getPartition(sf: SimpleFeature): Partition = {
    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    Partition(fmt.format(instant))
  }
}

class IntraHourPartitionScheme(minuteIntervals: Int, fmt: DateTimeFormatter, sft: SimpleFeatureType, partitionAttribute: String)
  extends PartitionScheme {
  private val index = sft.indexOf(partitionAttribute)
  override def getPartition(sf: SimpleFeature): Partition = {
    val instant = sf.getAttribute(index).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC)
    val adjusted = instant.withMinute(minuteIntervals*(instant.getMinute/minuteIntervals))
    Partition(fmt.format(adjusted))
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
}

class HierarchicalPartitionScheme(partitionSchemes: Seq[PartitionScheme], sep: String) extends PartitionScheme {
  override def getPartition(sf: SimpleFeature): Partition =
    Partition(partitionSchemes.map(_.getPartition(sf).name).mkString(sep))
}

object PartitionScheme {

  def apply(): PartitionScheme = ???
}
