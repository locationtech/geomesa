/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
    Seq(now, customFormatDateParser, datetime, basicDateTimeNoMillis, basicIsoDate, basicIsoDateTime, isoLocalDate,
      isoLocalDateTime, isoOffsetDateTime, dateHourMinuteSecondMillis, millisToDate, secsToDate, dateToString)

  private val now = TransformerFunction("now") { _ =>
    Date.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
  }

  private val millisToDate = TransformerFunction.pure("millisToDate") { args =>
    new Date(args(0).asInstanceOf[Long])
  }

  private val secsToDate = TransformerFunction.pure("secsToDate") { args =>
    new Date(args(0).asInstanceOf[Long] * 1000L)
  }

  // yyyy-MM-dd'T'HH:mm:ss.SSSZZ (ZZ is time zone with colon)
  private val datetime = new StandardDateParser(Seq("dateTime", "datetime")) {
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

  // TODO https://geomesa.atlassian.net/browse/GEOMESA-2326 update 'isodate' alias
  // even though this parser is aliased to 'isodate', it doesn't correspond with 'DateTimeFormatter.ISO_DATE',
  // which is '2011-12-03' or '2011-12-03+01:00'

  // yyyyMMdd
  private val basicIsoDate = new StandardDateParser(Seq("basicIsoDate", "basicDate", "isodate")) {
    override val format: DateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE.withZone(ZoneOffset.UTC)
  }

  // yyyy-MM-dd
  private val isoLocalDate = new StandardDateParser(Seq("isoLocalDate")) {
    override val format: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC)
  }

  // yyyy-MM-dd'T'HH:mm:ss
  private val isoLocalDateTime = new StandardDateParser(Seq("isoLocalDateTime")) {
    override val format: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneOffset.UTC)
  }

  // yyyy-MM-dd'T'HH:mm:ssZ
  private val isoOffsetDateTime = new StandardDateParser(Seq("isoOffsetDateTime")) {
    override val format: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC)
  }

  // TODO even though it is aliased to 'isodatetime', it doesn't correspond with 'DateTimeFormatter.ISO_DATE_TIME',
  // which is '2011-12-03T10:15:30', '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30+01:00[Europe/Paris]'

  // yyyyMMdd'T'HHmmss.SSSZ
  private val basicIsoDateTime = new StandardDateParser(Seq("basicDateTime", "isodatetime")) {
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

  private val customFormatDateParser = new CustomFormatDateParser()

  private val dateToString = new DateToString()
}

object DateFunctionFactory {

  abstract class StandardDateParser(names: Seq[String]) extends NamedTransformerFunction(names, pure = true) {
    val format: DateTimeFormatter
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      DateParsing.parseDate(args(0).toString, format)
  }

  class CustomFormatDateParser extends NamedTransformerFunction(Seq("date"), pure = true) {

    private var format: DateTimeFormatter = _

    override def getInstance: CustomFormatDateParser = new CustomFormatDateParser()

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (format == null) {
        format = DateTimeFormatter.ofPattern(args(0).asInstanceOf[String]).withZone(ZoneOffset.UTC)
      }
      DateParsing.parseDate(args(1).toString, format)
    }
  }

  class DateToString extends NamedTransformerFunction(Seq("dateToString"), pure = true) {

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
