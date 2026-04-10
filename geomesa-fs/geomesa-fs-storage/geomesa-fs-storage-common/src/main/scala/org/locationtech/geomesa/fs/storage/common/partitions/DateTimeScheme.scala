/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.calrissian.mango.types.LexiTypeEncoders
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionRange, RangeBuilder}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionKey
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.text.DateParsing

import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.{Date, Locale}

case class DateTimeScheme(dtg: String, dtgIndex: Int, unit: ChronoUnit, step: Int = 1) extends PartitionScheme {

  import FilterHelper.ff

  private val encoder = LexiTypeEncoders.integerEncoder()

  override val name: String = {
    val stepOpt = if (step == 1) { "" } else { s":step=$step"}
    s"${unit.name().toLowerCase(Locale.US)}:attribute=$dtg$stepOpt"
  }

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val instant = feature.getAttribute(dtgIndex).asInstanceOf[Date].toInstant
    PartitionKey(name, encoder.encode(toPartition(ZonedDateTime.ofInstant(instant, ZoneOffset.UTC))))
  }

  override def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]] = {
    getBounds(filter).map { bounds =>
      val builder = new RangeBuilder()
      bounds.foreach { range =>
        val lower = range.lower.value.fold("")(v => encoder.encode(toPartition(v)))
        val upper = exclusiveUpperBound(range).fold(DateTimeScheme.UnboundedUpper)(v => encoder.encode(v))
        builder += PartitionRange(name, lower, upper)
      }
      builder.result()
    }
  }

  override def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]] = {
    getBounds(filter).map { bounds =>
      bounds.flatMap { bound =>
        if (!bound.isBoundedBothSides) {
          throw new IllegalArgumentException(s"Can't enumerate an unbounded filter: ${ECQL.toCQL(filter)}")
        }
        val lower = toPartition(bound.lower.value.get)
        val upper = exclusiveUpperBound(bound).get
        Range(lower, upper).map(v => PartitionKey(name, encoder.encode(v)))
      }
    }
  }

  override def getCoveringFilter(partition: PartitionKey): Filter = {
    val offset = encoder.decode(partition.value) * step
    val start = DateTimeScheme.Epoch.plus(offset.longValue(), unit)
    val end = ff.literal(DateParsing.format(start.plus(step, unit)))
    ff.and(ff.greaterOrEqual(ff.property(dtg), ff.literal(DateParsing.format(start))), ff.less(ff.property(dtg), end))
  }

  private def toPartition(dt: ZonedDateTime): Int = unit.between(DateTimeScheme.Epoch, dt).toInt / step

  private def getBounds(filter: Filter): Option[Seq[Bounds[ZonedDateTime]]] = {
    val bounds = FilterHelper.extractIntervals(filter, dtg)
    if (bounds.isEmpty) {
      None
    } else if (bounds.disjoint) {
      Some(Seq.empty)
    } else {
      Some(bounds.values)
    }
  }

  private def exclusiveUpperBound(bounds: Bounds[ZonedDateTime]): Option[Int] = {
    bounds.upper.value.map { v =>
      val partition = toPartition(v)
      if (bounds.upper.exclusive && toPartition(v.minus(1, ChronoUnit.MILLIS)) < partition) {
        partition
      } else {
        partition + 1
      }
    }
  }
}

object DateTimeScheme {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val Epoch: ZonedDateTime = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)

  private val UnboundedUpper = "zzz"

  class DateTimePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
      val opts = SchemeOpts(scheme)
      val unit = opts.name match {
        case "year"  | "years"  | "yearly"  => Some(ChronoUnit.YEARS)
        case "month" | "months" | "monthly" => Some(ChronoUnit.MONTHS)
        case "week"  | "weeks"  | "weekly"  => Some(ChronoUnit.WEEKS)
        case "day"   | "days"   | "daily"   => Some(ChronoUnit.DAYS)
        case "hour"  | "hours"  | "hourly"  => Some(ChronoUnit.HOURS)
        case _ => None
      }
      unit.map { u =>
        val dtg = opts.getSingle("attribute").orElse(sft.getDtgField).orNull
        require(dtg != null, s"Date scheme requires an attribute to be specified with 'attribute=<attribute>'")
        val index = attributeIndex(sft, dtg, Some(classOf[Date]))
        val step = opts.getSingle("step").map(_.toInt).getOrElse(1)
        DateTimeScheme(dtg, index, u, step)
      }
    }
  }
}
