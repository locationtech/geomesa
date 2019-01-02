/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import java.time._
import java.time.temporal.{ChronoField, TemporalAccessor}
import java.util.Date

import org.geotools.factory.Hints
import org.geotools.util.{Converter, ConverterFactory}
import org.locationtech.geomesa.utils.text.DateParsing

class JavaTimeConverterFactory extends ConverterFactory {
  def createConverter(source: Class[_], target: Class[_], hints: Hints): Converter = {
    if (classOf[Date].isAssignableFrom(source) && classOf[String].isAssignableFrom(target)) {
      JavaTimeConverterFactory.DateToStringConverter
    } else if (classOf[Date].isAssignableFrom(target)) {
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
    } else {
      null
    }
  }
}

object JavaTimeConverterFactory {

  private val DateToStringConverter = new Converter {
    override def convert[T](source: AnyRef, target: Class[T]): T =
      DateParsing.formatDate(source.asInstanceOf[Date]).asInstanceOf[T]
  }

  private val StringToDateConverter = new Converter {
    override def convert[T](source: AnyRef, target: Class[T]): T =
      DateParsing.parseDate(source.asInstanceOf[String]).asInstanceOf[T]
  }

  private val TemporalToDateConverter = new Converter {
    override def convert[T](source: AnyRef, target: Class[T]): T = {
      val accessor = source.asInstanceOf[TemporalAccessor]
      val millis = accessor.getLong(ChronoField.INSTANT_SECONDS) * 1000 +
          accessor.getLong(ChronoField.MILLI_OF_SECOND)
      new Date(millis).asInstanceOf[T]
    }
  }

  private val LocalDateToDateConverter = new Converter {
    override def convert[T](source: AnyRef, target: Class[T]): T =
      TemporalToDateConverter.convert(source.asInstanceOf[LocalDate].atStartOfDay(ZoneOffset.UTC), target)
  }

  private val LocalDateTimeToDateConverter = new Converter {
    override def convert[T](source: AnyRef, target: Class[T]): T =
      TemporalToDateConverter.convert(source.asInstanceOf[LocalDateTime].atZone(ZoneOffset.UTC), target)
  }
}