/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.{PartitionSpec, StructLike}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.core.schema.ColumnName
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionScheme.{TemporalPartition, TemporalScheme}
import org.locationtech.geomesa.utils.text.DateParsing

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, ZoneOffset, ZonedDateTime}
import java.util.{Date, Locale}

/**
 * Date based partitioning
 *
 * @param attribute attribute being partitioned
 * @param index index in the feature type of the attribute
 * @param unit chrono unit used for partitioning
 */
case class DateTimeScheme(attribute: String, index: Int, unit: ChronoUnit) extends PartitionScheme with TemporalScheme {

  import FilterHelper.ff

  override val name: String = s"${unit.name().toLowerCase(Locale.US)}:attribute=$attribute"

  private val encoder = DateTimeScheme.encoder(unit)

  override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = unit match {
    case ChronoUnit.HOURS  => b.hour(ColumnName.encode(attribute))
    case ChronoUnit.DAYS   => b.day(ColumnName.encode(attribute))
    case ChronoUnit.MONTHS => b.month(ColumnName.encode(attribute))
    case ChronoUnit.YEARS  => b.year(ColumnName.encode(attribute))
    case _ => throw new UnsupportedOperationException("An implementation is missing")
  }

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val instant = feature.getAttribute(index).asInstanceOf[Date].toInstant
    val value = unit.between(DateTimeScheme.Epoch, ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)).toInt
    PartitionKey(name, encoder.encode(value))
  }

  override def getCoveringFilter(partition: PartitionKey): Filter = {
    val offset = encoder.decode(partition.value)
    val start = DateTimeScheme.Epoch.plus(offset.longValue(), unit)
    val end = ff.literal(DateParsing.format(start.plus(1, unit)))
    ff.and(ff.greaterOrEqual(ff.property(attribute), ff.literal(DateParsing.format(start))), ff.less(ff.property(attribute), end))
  }

  override def getCoveringExpression(partition: PartitionKey): Expression = {
    val transform = unit match {
      case ChronoUnit.HOURS  => Expressions.hour[Integer](ColumnName.encode(attribute))
      case ChronoUnit.DAYS   => Expressions.day[Integer](ColumnName.encode(attribute))
      case ChronoUnit.MONTHS => Expressions.month[Integer](ColumnName.encode(attribute))
      case ChronoUnit.YEARS  => Expressions.year[Integer](ColumnName.encode(attribute))
      case _ => throw new UnsupportedOperationException("An implementation is missing")
    }
    Expressions.equal[Integer](transform, encoder.decode(partition.value))
  }

  override def getPartition(partition: StructLike, i: Int): PartitionKey = PartitionKey(name, encoder.getPartition(partition, i))

  override def partition(time: ZonedDateTime): TemporalPartition = {
    val value = unit.between(DateTimeScheme.Epoch, time).toInt
    val partition = PartitionKey(name, encoder.encode(value))
    val start = DateTimeScheme.Epoch.plus(value, unit)
    val end = start.plus(1, unit)
    TemporalPartition(partition, start, end)
  }
}

object DateTimeScheme extends PartitionSchemeFactory {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val Epoch: ZonedDateTime = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)

  override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
    val opts = SchemeOpts(scheme)
    val unit = opts.name match {
      case "year"  | "years"  | "yearly"  => Some(ChronoUnit.YEARS)
      case "month" | "months" | "monthly" => Some(ChronoUnit.MONTHS)
      case "day"   | "days"   | "daily"   => Some(ChronoUnit.DAYS)
      case "hour"  | "hours"  | "hourly"  => Some(ChronoUnit.HOURS)
      case _ => None
    }
    unit.map { u =>
      val dtg = opts.getSingle("attribute").orElse(sft.getDtgField).orNull
      require(dtg != null, s"Date scheme requires an attribute to be specified with 'attribute=<attribute>'")
      val index = attributeIndex(sft, dtg, Some(classOf[Date]))
      if (opts.getSingle("step").exists(_ != "1")) {
        throw new IllegalArgumentException("`step` argument is no longer supported in date-time schemes")
      }
      DateTimeScheme(dtg, index, u)
    }
  }

  private def encoder(unit: ChronoUnit): PartitionEncoder = {
    unit match {
      case ChronoUnit.DAYS => DayEncoder
      case _ => DefaultEncoder
    }
  }

  private sealed trait PartitionEncoder {
    def encode(value: Int): String
    def decode(value: String): Int
    def getPartition(partition: StructLike, i: Int): String
  }

  private object DefaultEncoder extends PartitionEncoder {
    override def encode(value: Int): String = value.toString
    override def decode(value: String): Int = value.toInt
    override def getPartition(partition: StructLike, i: Int): String = partition.get(i, classOf[Integer]).toString
  }

  // note: days are handled differently from other types, and expect an ISO_LOCAL_DATE formatted string
  private object DayEncoder extends PartitionEncoder {
    override def encode(value: Int): String = DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDate.EPOCH.plusDays(value))
    override def decode(value: String): Int = {
      val date = DateParsing.parse(value, DateTimeFormatter.ISO_LOCAL_DATE)
      ChronoUnit.DAYS.between(DateTimeScheme.Epoch, date).toInt
    }
    override def getPartition(partition: StructLike, i: Int): String = encode(partition.get(i, classOf[Integer]))
  }
}
