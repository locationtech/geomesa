/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.DateFunctionFactory.{CustomFormatDateParser, DateToString, StandardDateParser}
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.utils.text.DateParsing

class DateFunctionFactory extends TransformerFunctionFactory {

  import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
  import java.time.temporal.ChronoField
  import java.time.{ZoneOffset, ZonedDateTime}

  override def functions: Seq[TransformerFunction] =
    Seq(now, customFormatDateParser, datetime, isodate, isodatetime, basicDateTimeNoMillis,
      dateHourMinuteSecondMillis, millisToDate, secsToDate, dateToString)

  private val now = TransformerFunction("now") { _ =>
    Date.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
  }

  private val millisToDate = TransformerFunction("millisToDate") { args =>
    new Date(args(0).asInstanceOf[Long])
  }

  private val secsToDate = TransformerFunction("secsToDate") { args =>
    new Date(args(0).asInstanceOf[Long] * 1000L)
  }

  // yyyy-MM-dd'T'HH:mm:ss.SSSZZ (ZZ is time zone with colon)
  private val datetime = new StandardDateParser(Seq("datetime", "dateTime")) {
    override val format: DateTimeFormatter =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .parseLenient()
          .appendLiteral('T')
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, true)
          .optionalStart()
          .appendOffsetId()
          .toFormatter(Locale.US)
          .withZone(ZoneOffset.UTC)
  }

  // yyyyMMdd
  private val isodate = new StandardDateParser(Seq("isodate", "basicDate")) {
    override val format: DateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE.withZone(ZoneOffset.UTC)
  }

  // yyyyMMdd'T'HHmmss.SSSZ
  private val isodatetime = new StandardDateParser(Seq("isodatetime", "basicDateTime")) {
    override val format: DateTimeFormatter =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .appendValue(ChronoField.YEAR, 4)
          .appendValue(ChronoField.MONTH_OF_YEAR, 2)
          .appendValue(ChronoField.DAY_OF_MONTH, 2)
          .appendLiteral('T')
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, true)
          .optionalStart()
          .appendOffsetId()
          .toFormatter(Locale.US)
          .withZone(ZoneOffset.UTC)
  }

  // yyyyMMdd'T'HHmmssZ
  private val basicDateTimeNoMillis = new StandardDateParser(Seq("basicDateTimeNoMillis")) {
    override val format: DateTimeFormatter =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .appendValue(ChronoField.YEAR, 4)
          .appendValue(ChronoField.MONTH_OF_YEAR, 2)
          .appendValue(ChronoField.DAY_OF_MONTH, 2)
          .appendLiteral('T')
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendOffsetId()
          .toFormatter(Locale.US)
          .withZone(ZoneOffset.UTC)
  }

  // yyyy-MM-dd'T'HH:mm:ss.SSS
  private val dateHourMinuteSecondMillis =
    new StandardDateParser(Seq("dateHourMinuteSecondMillis")) {
      override val format: DateTimeFormatter =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .parseLenient()
            .appendLiteral('T')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendFraction(ChronoField.MILLI_OF_SECOND, 3, 3, true)
            .toFormatter(Locale.US)
            .withZone(ZoneOffset.UTC)
    }

  // TODO add more default dates for type inference

  private val customFormatDateParser = new CustomFormatDateParser()

  private val dateToString = new DateToString()
}

object DateFunctionFactory {

  abstract class StandardDateParser(names: Seq[String]) extends NamedTransformerFunction(names) {
    val format: DateTimeFormatter
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      DateParsing.parseDate(args(0).toString, format)
  }

  class CustomFormatDateParser extends NamedTransformerFunction(Seq("date")) {

    private var format: DateTimeFormatter = _

    override def getInstance: CustomFormatDateParser = new CustomFormatDateParser()

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (format == null) {
        format = DateTimeFormatter.ofPattern(args(0).asInstanceOf[String]).withZone(ZoneOffset.UTC)
      }
      DateParsing.parseDate(args(1).toString, format)
    }
  }

  class DateToString extends NamedTransformerFunction(Seq("dateToString")) {

    private var format: DateTimeFormatter = _

    override def getInstance: DateToString = new DateToString()

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (format == null) {
        format = DateTimeFormatter.ofPattern(args(0).asInstanceOf[String]).withZone(ZoneOffset.UTC)
      }
      DateParsing.formatDate(args(1).asInstanceOf[java.util.Date], format)
    }
  }
}
