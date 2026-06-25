/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.DateParsing
import org.specs2.mutable.SpecificationWithJUnit

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.Date

class DateTimeSchemeTest extends SpecificationWithJUnit {

  import org.locationtech.geomesa.filter.decomposeAnd

  val sft = SimpleFeatureTypes.createType("test", "dtg:Date,*geom:Point:srid=4326")
  val sf = ScalaSimpleFeature.create(sft, "1", "2017-02-03T10:15:30Z", "POINT (10 10)")
  val date = ZonedDateTime.ofInstant(sf.getAttribute(0).asInstanceOf[Date].toInstant, ZoneOffset.UTC)

  val epoch = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)

  "DateTimeScheme" should {

    "partition based on hours" >> {
      val ps = DateTimeScheme("dtg", 0, ChronoUnit.HOURS)
      val partition = ps.getPartition(sf)
      partition.value mustEqual "412810"
      val hours = partition.value.toInt
      ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC).plusHours(hours) mustEqual truncate(date, ChronoUnit.HOURS)
    }

    "partition based on days" >> {
      val ps = DateTimeScheme("dtg", 0, ChronoUnit.DAYS)
      val partition = ps.getPartition(sf)
      partition.value mustEqual "2017-02-03"
      DateParsing.parse(partition.value, DateTimeFormatter.ISO_LOCAL_DATE) mustEqual truncate(date, ChronoUnit.DAYS)
    }

    "calculate covering filters for partitions" >> {
      foreach(Seq(ChronoUnit.HOURS, ChronoUnit.DAYS, ChronoUnit.WEEKS, ChronoUnit.MONTHS, ChronoUnit.YEARS)) { unit =>
        val ps = DateTimeScheme("dtg", 0, unit)
        val partition = ps.getPartition(sf)
        val covering = ps.getCoveringFilter(partition)
        val expected = {
          val start = truncate(date, unit)
          val end = start.plus(1, unit)
          ECQL.toFilter(s"dtg >= '${DateParsing.format(start)}' AND dtg < '${DateParsing.format(end)}'")
        }
        decomposeAnd(covering) must containTheSameElementsAs(decomposeAnd(expected))
      }
    }
  }

  private def truncate(date: ZonedDateTime, unit: ChronoUnit, step: Int = 1): ZonedDateTime = {
    val base = unit match {
      case ChronoUnit.HOURS | ChronoUnit.DAYS => date.truncatedTo(unit)
      case _ => epoch.plus(unit.between(epoch, date), unit)
    }
    if (step == 1) { base } else {
      base.minus(unit.between(epoch, base).toInt % step, unit)
    }
  }
}
