/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import java.time.temporal.ChronoUnit

import org.joda.time._
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BinnedTimeTest extends Specification {

  val JodaEpoch = new DateTime(0, DateTimeZone.UTC)

  val rand = new Random(-574)

  val times = Seq.fill(10) {
    BinnedTime.Epoch
        .plusYears(rand.nextInt(40))
        .plusMonths(rand.nextInt(12))
        .plusDays(rand.nextInt(28))
        .plusHours(rand.nextInt(24))
        .plusMinutes(rand.nextInt(60))
        .plusSeconds(rand.nextInt(60))
        .plusNanos(rand.nextInt(1000) * 1000 * 1000)
  }

  def getDaysAndMillis_old(date: DateTime): BinnedTime = {
    val days = Days.daysBetween(JodaEpoch, date)
    val millisInDay = date.getMillis - JodaEpoch.plus(days).getMillis
    BinnedTime(days.getDays.toShort, millisInDay)
  }

  def getWeeksAndSeconds_old(date: DateTime): BinnedTime = {
    val weeks = Weeks.weeksBetween(JodaEpoch, date)
    val secondsInWeek = (date.getMillis - JodaEpoch.plus(weeks).getMillis) / 1000L
    BinnedTime(weeks.getWeeks.toShort, secondsInWeek)
  }

  def getMonthsAndSeconds_old(date: DateTime): BinnedTime = {
    val months = Months.monthsBetween(JodaEpoch, date)
    val secondsInMonth = (date.getMillis - JodaEpoch.plus(months).getMillis) / 1000L
    BinnedTime(months.getMonths.toShort, secondsInMonth)
  }

  def getYearsAndMinutes_old(date: DateTime): BinnedTime = {
    val years = Years.yearsBetween(JodaEpoch, date)
    val minutesInYear = (date.getMillis - JodaEpoch.plus(years).getMillis) / 60000L
    BinnedTime(years.getYears.toShort, minutesInYear)
  }

  "BinnedTime" should {

    "convert to weeks and seconds and back" >> {
      forall(times) { time =>
        val binned = BinnedTime.dateToBinnedTime(TimePeriod.Week)(time)
        BinnedTime.binnedTimeToDate(TimePeriod.Week)(binned) mustEqual time.minus(time.getNano / 1000000, ChronoUnit.MILLIS)
      }
    }

    "convert to days and millis and back" >> {
      forall(times) { time =>
        val binned = BinnedTime.dateToBinnedTime(TimePeriod.Day)(time)
        BinnedTime.binnedTimeToDate(TimePeriod.Day)(binned) mustEqual time
      }
    }

    "convert to months and seconds and back" >> {
      forall(times) { time =>
        val binned = BinnedTime.dateToBinnedTime(TimePeriod.Month)(time)
        BinnedTime.binnedTimeToDate(TimePeriod.Month)(binned) mustEqual time.minus(time.getNano / 1000000, ChronoUnit.MILLIS)
      }
    }

    "convert to years and minutes and back" >> {
      forall(times) { time =>
        val binned = BinnedTime.dateToBinnedTime(TimePeriod.Year)(time)
        BinnedTime.binnedTimeToDate(TimePeriod.Year)(binned) mustEqual
            time.minus(time.getNano / 1000000, ChronoUnit.MILLIS).minusSeconds(time.getSecond)
      }
    }

    "convert to days and millis back compatible" >> {
      forall(times) { time =>
        val bin = BinnedTime.dateToBinnedTime(TimePeriod.Day)(time)
        getDaysAndMillis_old(new DateTime(time.toInstant.toEpochMilli, DateTimeZone.UTC)) mustEqual bin
      }
    }

    "convert to weeks and seconds back compatible" >> {
      forall(times) { time =>
        val bin = BinnedTime.dateToBinnedTime(TimePeriod.Week)(time)
        getWeeksAndSeconds_old(new DateTime(time.toInstant.toEpochMilli, DateTimeZone.UTC)) mustEqual bin
      }
    }

    "convert to months and seconds back compatible" >> {
      forall(times) { time =>
        val bin = BinnedTime.dateToBinnedTime(TimePeriod.Month)(time)
        getMonthsAndSeconds_old(new DateTime(time.toInstant.toEpochMilli, DateTimeZone.UTC)) mustEqual bin
      }
    }

    "convert to years and minutes back compatible" >> {
      forall(times) { time =>
        val bin = BinnedTime.dateToBinnedTime(TimePeriod.Year)(time)
        getYearsAndMinutes_old(new DateTime(time.toInstant.toEpochMilli, DateTimeZone.UTC)) mustEqual bin
      }
    }
  }
}
