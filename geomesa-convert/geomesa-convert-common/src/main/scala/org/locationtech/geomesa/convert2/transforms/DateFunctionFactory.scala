/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.convert2.transforms.Expression.LiteralString
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.utils.text.DateParsing

class DateFunctionFactory extends TransformerFunctionFactory {

  import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
  import java.time.temporal.ChronoField
  import java.time.{ZoneOffset, ZonedDateTime}

  override def functions: Seq[TransformerFunction] =
    Seq(now, customFormatDateParser, datetime, basicDateTimeNoMillis, basicIsoDate, basicDateTime, isoDate,
      isoLocalDate, isoLocalDateTime, isoOffsetDateTime, isoDateTime, dateHourMinuteSecondMillis,
      millisToDate, secsToDate, dateToString, dateToMillis)

  private val now = TransformerFunction("now") { _ =>
    Date.from(ZonedDateTime.now(ZoneOffset.UTC).toInstant)
  }

  private val millisToDate = TransformerFunction.pure("millisToDate") { args =>
    args(0) match {
      case null => null
      case d: Long => new Date(d)
      case d: Int  => new Date(d)
      case d => throw new IllegalArgumentException(s"Invalid millisecond: $d")
    }
  }

  private val secsToDate = TransformerFunction.pure("secsToDate") { args =>
    args(0) match {
      case null => null
      case d: Long => new Date(d * 1000L)
      case d: Int  => new Date(d * 1000L)
      case d => throw new IllegalArgumentException(s"Invalid second: $d")
    }
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

  // yyyyMMdd
  private val basicIsoDate = new StandardDateParser(Seq("basicIsoDate", "basicDate")) {
    override val format: DateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE.withZone(ZoneOffset.UTC)
  }

  // yyyy-MM-dd
  private val isoDate = new StandardDateParser(Seq("isoDate")) {
    override val format: DateTimeFormatter = DateTimeFormatter.ISO_DATE.withZone(ZoneOffset.UTC)
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

  // yyyy-MM-dd'T'HH:mm:ss
  private val isoDateTime = new StandardDateParser(Seq("isoDateTime")) {
    override val format: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneOffset.UTC)
  }

  // yyyyMMdd'T'HHmmss.SSSZ
  private val basicDateTime = new StandardDateParser(Seq("basicDateTime")) {
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

  private val customFormatDateParser = new CustomFormatDateParser(null)

  private val dateToString = new DateToString(null)

  private val dateToMillis = TransformerFunction.pure("dateToMillis") { args =>
    if (args(0) == null) { null } else { args(0).asInstanceOf[Date].getTime }
  }
}

object DateFunctionFactory {

  abstract class StandardDateParser(names: Seq[String]) extends NamedTransformerFunction(names, pure = true) {
    val format: DateTimeFormatter
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      args(0) match {
        case null => null
        case d: String => DateParsing.parseDate(d, format)
        case d => DateParsing.parseDate(d.toString, format)
      }
    }
  }

  class CustomFormatDateParser(format: DateTimeFormatter) extends NamedTransformerFunction(Seq("date"), pure = true) {

    override def getInstance(args: List[Expression]): CustomFormatDateParser = {
      val format = args match {
        case LiteralString(s) :: _ => DateTimeFormatter.ofPattern(s).withZone(ZoneOffset.UTC)
        case _ => throw new IllegalArgumentException(s"Expected date pattern but got: ${args.headOption.orNull}")
      }
      new CustomFormatDateParser(format)
    }

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      args(1) match {
        case null => null
        case d: String => DateParsing.parseDate(d, format)
        case d => DateParsing.parseDate(d.toString, format)
      }
    }
  }

  class DateToString(format: DateTimeFormatter) extends NamedTransformerFunction(Seq("dateToString"), pure = true) {

    override def getInstance(args: List[Expression]): DateToString = {
      val format = args match {
        case LiteralString(s) :: _ => DateTimeFormatter.ofPattern(s).withZone(ZoneOffset.UTC)
        case _ => throw new IllegalArgumentException(s"Expected date pattern but got: ${args.headOption.orNull}")
      }
      new DateToString(format)
    }

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      DateParsing.formatDate(args(1).asInstanceOf[java.util.Date], format)
  }
}
