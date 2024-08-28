/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import org.geotools.data.util.TemporalConverterFactory
import org.geotools.util.factory.Hints
import org.geotools.util.{Converter, ConverterFactory}
import org.locationtech.geomesa.utils.geotools.converters.JavaTimeConverterFactory.TwoStageTemporalConverter
import org.locationtech.geomesa.utils.text.DateParsing

import java.time._
import java.time.temporal.{ChronoField, TemporalAccessor}
import java.util.Date

class JavaTimeConverterFactory extends ConverterFactory {
  def createConverter(source: Class[_], target: Class[_], hints: Hints): Converter = {
    if (classOf[Date].isAssignableFrom(source) && classOf[String] == target) {
      JavaTimeConverterFactory.DateToStringConverter
    } else if (classOf[Date].isAssignableFrom(target)) {
      val toDate =
        if (classOf[String].isAssignableFrom(source)) {
          JavaTimeConverterFactory.StringToDateConverter
        } else if (classOf[ZonedDateTime].isAssignableFrom(source) || classOf[Instant].isAssignableFrom(source)
            || classOf[OffsetDateTime].isAssignableFrom(source)) {
          JavaTimeConverterFactory.TemporalToDateConverter
        } else if (classOf[LocalDateTime].isAssignableFrom(source)) {
          JavaTimeConverterFactory.LocalDateTimeToDateConverter
        } else if (classOf[LocalDate].isAssignableFrom(source)) {
          JavaTimeConverterFactory.LocalDateToDateConverter
        } else {
          null
        }
      if (toDate == null || classOf[Date] == target) {
        toDate
      } else {
        val toTarget = new TemporalConverterFactory().createConverter(classOf[Date], target, hints)
        if (toTarget == null) {
          null
        } else {
          new TwoStageTemporalConverter(toDate, toTarget)
        }
      }
    } else {
      null
    }
  }
}

object JavaTimeConverterFactory {

  private trait DateConverter extends Converter {

    override def convert[T](source: AnyRef, target: Class[T]): T = convert(source).asInstanceOf[T]

    def convert(source: AnyRef): Date
  }

  private object DateToStringConverter extends Converter {
    override def convert[T](source: AnyRef, target: Class[T]): T =
      DateParsing.formatDate(source.asInstanceOf[Date]).asInstanceOf[T]
  }

  private object StringToDateConverter extends DateConverter {
    override def convert(source: AnyRef): Date = DateParsing.parseDate(source.asInstanceOf[String])
  }

  private object TemporalToDateConverter extends DateConverter {
    override def convert(source: AnyRef): Date = {
      val accessor = source.asInstanceOf[TemporalAccessor]
      val millis = accessor.getLong(ChronoField.INSTANT_SECONDS) * 1000 +
          accessor.getLong(ChronoField.MILLI_OF_SECOND)
      new Date(millis)
    }
  }

  private object LocalDateToDateConverter extends DateConverter {
    override def convert(source: AnyRef): Date =
      TemporalToDateConverter.convert(source.asInstanceOf[LocalDate].atStartOfDay(ZoneOffset.UTC))
  }

  private object LocalDateTimeToDateConverter extends DateConverter {
    override def convert(source: AnyRef): Date =
      TemporalToDateConverter.convert(source.asInstanceOf[LocalDateTime].atZone(ZoneOffset.UTC))
  }

  private class TwoStageTemporalConverter(toDate: DateConverter, toTarget: Converter) extends Converter {
    override def convert[T](source: AnyRef, target: Class[T]): T =
      toTarget.convert(toDate.convert(source), target)
  }
}