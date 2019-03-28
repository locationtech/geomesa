/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import org.locationtech.geomesa.utils.date.DateUtils.toInstant

import java.time._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.{ChronoField, TemporalAccessor, TemporalQuery}
import java.util.{Date, Locale}

object DateParsing {

  private val format =
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .parseLenient()
      .optionalStart()
      .appendLiteral('T')
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .optionalStart()
      .appendLiteral(':')
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .optionalStart()
      .appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, true)
      .optionalEnd()
      .optionalEnd()
      .optionalEnd()
      .optionalStart()
      .appendOffsetId()
      .toFormatter(Locale.US)
      .withZone(ZoneOffset.UTC)

  val Epoch: ZonedDateTime = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)

  object TemporalQueries {
    val ZonedQuery: TemporalQuery[ZonedDateTime] = new TemporalQuery[ZonedDateTime] {
      override def queryFrom(temporal: TemporalAccessor): ZonedDateTime = ZonedDateTime.from(temporal)
    }
    val LocalQuery: TemporalQuery[LocalDateTime] = new TemporalQuery[LocalDateTime] {
      override def queryFrom(temporal: TemporalAccessor): LocalDateTime = LocalDateTime.from(temporal)
    }
    val LocalDateQuery: TemporalQuery[LocalDate] = new TemporalQuery[LocalDate] {
      override def queryFrom(temporal: TemporalAccessor): LocalDate = LocalDate.from(temporal)
    }
  }

  /**
    * Parses a date string, with optional time and zone
    *
    * @param value date string
    * @param format date formatter, default ISO format with optional time and zone
    * @return
    */
  def parse(value: String, format: DateTimeFormatter = format): ZonedDateTime = {
    import TemporalQueries.{LocalDateQuery, LocalQuery, ZonedQuery}
    format.parseBest(value, ZonedQuery, LocalQuery, LocalDateQuery) match {
      case d: ZonedDateTime => d
      case d: LocalDateTime => d.atZone(ZoneOffset.UTC)
      case d: LocalDate     => d.atTime(LocalTime.MIN).atZone(ZoneOffset.UTC)
    }
  }

  /**
    * Parses a date string, with optional time and zone
    *
    * @param value date string
    * @param format date formatter, default ISO format with optional time and zone
    * @return
    */
  def parseInstant(value: String, format: DateTimeFormatter = format): Instant = {
    import TemporalQueries.{LocalDateQuery, LocalQuery, ZonedQuery}
    format.parseBest(value, ZonedQuery, LocalQuery, LocalDateQuery) match {
      case d: ZonedDateTime => d.toInstant
      case d: LocalDateTime => d.toInstant(ZoneOffset.UTC)
      case d: LocalDate     => d.atTime(LocalTime.MIN).toInstant(ZoneOffset.UTC)
    }
  }

  /**
    * Parses a date string, with optional time and zone
    *
    * @param value date string
    * @param format date formatter, default ISO format with optional time and zone
    * @return
    */
  def parseDate(value: String, format: DateTimeFormatter = format): Date = {
    import TemporalQueries.{LocalDateQuery, LocalQuery, ZonedQuery}
    format.parseBest(value, ZonedQuery, LocalQuery, LocalDateQuery) match {
      case d: ZonedDateTime => Date.from(d.toInstant)
      case d: LocalDateTime => Date.from(d.toInstant(ZoneOffset.UTC))
      case d: LocalDate     => Date.from(d.atTime(LocalTime.MIN).toInstant(ZoneOffset.UTC))
    }
  }

  /**
    * Parses a date string, with optional time and zone
    *
    * @param value date string
    * @param format date formatter, default ISO format with optional time and zone
    * @return
    */
  def parseMillis(value: String, format: DateTimeFormatter = format): Long = {
    import TemporalQueries.{LocalDateQuery, LocalQuery, ZonedQuery}
    format.parseBest(value, ZonedQuery, LocalQuery, LocalDateQuery) match {
      case d: ZonedDateTime => d.toInstant.toEpochMilli
      case d: LocalDateTime => d.toInstant(ZoneOffset.UTC).toEpochMilli
      case d: LocalDate     => d.atTime(LocalTime.MIN).toInstant(ZoneOffset.UTC).toEpochMilli
    }
  }

  def format(value: ZonedDateTime, format: DateTimeFormatter = format): String = value.format(format)

  def formatDate(value: Date, format: DateTimeFormatter = format): String =
    ZonedDateTime.ofInstant(toInstant(value), ZoneOffset.UTC).format(format)

  def formatInstant(value: Instant, format: DateTimeFormatter = format): String =
    ZonedDateTime.ofInstant(value, ZoneOffset.UTC).format(format)

  def formatMillis(value: Long, format: DateTimeFormatter = format): String =
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC).format(format)
}
