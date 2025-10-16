/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import org.geotools.util.{Converter, Converters}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import java.util.Date

@RunWith(classOf[JUnitRunner])
class JavaTimeConverterFactoryTest extends Specification {

  import scala.collection.JavaConverters._

  val dStr = "2015-01-01T00:00:00.000Z"
  val date = Date.from(ZonedDateTime.parse(dStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant)
  require(date != null)

  private def createConverter(from: Class[_], to: Class[_]): Converter = {
    // note: we don't call Converters.getConverterFactories(from, to) as that tries to load jai-dependent classes
    val factory = Converters.getConverterFactories(null).asScala.find(_.isInstanceOf[JavaTimeConverterFactory]).orNull
    factory must not(beNull)
    factory.createConverter(from, to, null)
  }

  "JavaTimeConverter" should {

    "convert a range of ISO8601 strings to dates" >> {
      val converter = createConverter(classOf[String], classOf[java.util.Date])
      converter.convert("2015-01-01T00:00:00.000Z", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01T00:00:00.000", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01T00:00:00Z", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01T00:00:00", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01T00:00Z", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01T00:00", classOf[java.util.Date]) mustEqual date
      converter.convert("2015-01-01", classOf[java.util.Date]) mustEqual date
    }

    "convert a date to a full ISO8601 string" >> {
      val converter = createConverter(classOf[Date], classOf[String])
      converter.convert(date, classOf[String]) mustEqual "2015-01-01T00:00:00.000Z"
    }

    "convert java time classes to dates" >> {
      val times = Seq(
        Instant.ofEpochMilli(date.getTime),
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC),
        LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC),
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC),
        LocalDate.from(ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC))
      )
      foreach(times) { time =>
        createConverter(time.getClass, classOf[Date]).convert(time, classOf[Date]) mustEqual date
      }
    }

    "convert java time classes to sql dates" >> {
      val times = Seq(
        Instant.ofEpochMilli(date.getTime),
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC),
        LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC),
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC),
        LocalDate.from(ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), ZoneOffset.UTC))
      )
      foreach(times) { time =>
        val result = createConverter(time.getClass, classOf[Timestamp]).convert(time, classOf[Timestamp])
        result must beAnInstanceOf[Timestamp]
        date mustEqual result // compare backwards (expected vs actual) so that equals check works
      }
    }
  }
}
