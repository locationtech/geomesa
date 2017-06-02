/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import java.util.Date

import org.joda.time._
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod

/**
  * Represents a time by an offset into a binned bucket. The bin represents days, weeks,
  * months or years since the java epoch. The offset represents milliseconds, seconds, or
  * hours into that bin.
  *
  * Times can be partitioned based on four periods:
  *
  *   TimePeriod.Day
  *     bin      => day
  *     offset   => milliseconds
  *     max date => 2059/09/18
  *
  *   TimePeriod.Week
  *     bin      => week
  *     offset   => seconds
  *     max date => 2598/01/04
  *
  *   TimePeriod.Month
  *     bin      => month
  *     offset   => seconds
  *     max date => 4700/08/31
  *
  *   TimePeriod.Year
  *     bin      => year
  *     offset   => minutes
  *     max date => 34737/12/31
  *
  * @param bin number of time periods from the java epoch
  * @param offset precise offset into the specific time period
  */
case class BinnedTime(bin: Short, offset: Long)

object BinnedTime {

  type TimeToBinnedTime = (Long) => BinnedTime
  type DateToBinnedTime = (DateTime) => BinnedTime
  type BinnedTimeToDate = (BinnedTime) => DateTime

  val Epoch = new DateTime(0, DateTimeZone.UTC)
  val ZMinDate: Date = Epoch.toDate


  val DaysMaxDate   = Epoch.plus(Days.days(Short.MaxValue.toInt + 1))
  val WeeksMaxDate  = Epoch.plus(Weeks.weeks(Short.MaxValue.toInt + 1))
  val MonthsMaxDate = Epoch.plus(Months.months(Short.MaxValue.toInt + 1))
  val YearsMaxDate  = Epoch.plus(Years.years(Short.MaxValue.toInt + 1))

  /**
    * Gets period index (e.g. weeks since the epoch) and offset into that interval (e.g. seconds in week)
    *
    * @param period interval type
    * @return
    */
  def timeToBinnedTime(period: TimePeriod): TimeToBinnedTime = {
    period match {
      case TimePeriod.Day   => toDayAndMillis
      case TimePeriod.Week  => toWeekAndSeconds
      case TimePeriod.Month => toMonthAndSeconds
      case TimePeriod.Year  => toYearAndMinutes
    }
  }

  /**
    * Gets period index (e.g. weeks since the epoch) and offset into that interval (e.g. seconds in week)
    *
    * @param period interval type
    * @return
    */
  def dateToBinnedTime(period: TimePeriod): DateToBinnedTime = {
    period match {
      case TimePeriod.Day   => toDayAndMillis
      case TimePeriod.Week  => toWeekAndSeconds
      case TimePeriod.Month => toMonthAndSeconds
      case TimePeriod.Year  => toYearAndMinutes
    }
  }

  /**
    * Gets a date back from a binned time
    *
    * @param period interval type
    * @return
    */
  def binnedTimeToDate(period: TimePeriod): BinnedTimeToDate = {
    period match {
      case TimePeriod.Day   => fromDayAndMillis
      case TimePeriod.Week  => fromWeekAndSeconds
      case TimePeriod.Month => fromMonthAndSeconds
      case TimePeriod.Year  => fromYearAndMinutes
    }
  }

  /**
    * Gets the max offset value for a given time period
    *
    * @param period interval type
    * @return
    */
  def maxOffset(period: TimePeriod): Long = {
    period match {
      case TimePeriod.Day   => Days.ONE.toStandardDuration.getMillis
      case TimePeriod.Week  => Weeks.ONE.toStandardDuration.getMillis / 1000L
      case TimePeriod.Month => (Days.ONE.toStandardDuration.getMillis / 1000L) * 31L
      case TimePeriod.Year  => (Weeks.ONE.toStandardDuration.getMillis / 60000L) * 52L
    }
  }

  def maxDate(period: TimePeriod): DateTime = {
    period match {
      case TimePeriod.Day   => DaysMaxDate
      case TimePeriod.Week  => WeeksMaxDate
      case TimePeriod.Month => MonthsMaxDate
      case TimePeriod.Year  => YearsMaxDate
    }
  }

  private def toDayAndMillis(time: Long): BinnedTime =
    toDayAndMillis(new DateTime(time, DateTimeZone.UTC))

  private def toDayAndMillis(date: DateTime): BinnedTime = {
    require(DaysMaxDate.isAfter(date), s"Date exceeds maximum indexable value ($DaysMaxDate): $date")
    val days = Days.daysBetween(Epoch, date)
    val millisInDay = date.getMillis - Epoch.plus(days).getMillis
    BinnedTime(days.getDays.toShort, millisInDay)
  }

  private def fromDayAndMillis(date: BinnedTime): DateTime = Epoch.plusDays(date.bin).plus(date.offset)

  private def toWeekAndSeconds(time: Long): BinnedTime =
    toWeekAndSeconds(new DateTime(time, DateTimeZone.UTC))

  private def toWeekAndSeconds(date: DateTime): BinnedTime = {
    require(WeeksMaxDate.isAfter(date), s"Date exceeds maximum indexable value ($WeeksMaxDate): $date")
    val weeks = Weeks.weeksBetween(Epoch, date)
    val secondsInWeek = (date.getMillis - Epoch.plus(weeks).getMillis) / 1000L
    BinnedTime(weeks.getWeeks.toShort, secondsInWeek)
  }

  private def fromWeekAndSeconds(date: BinnedTime): DateTime =
    Epoch.plusWeeks(date.bin).plus(date.offset * 1000L)

  private def toMonthAndSeconds(time: Long): BinnedTime =
    toMonthAndSeconds(new DateTime(time, DateTimeZone.UTC))

  private def toMonthAndSeconds(date: DateTime): BinnedTime = {
    require(MonthsMaxDate.isAfter(date), s"Date exceeds maximum indexable value ($MonthsMaxDate): $date")
    val months = Months.monthsBetween(Epoch, date)
    val secondsInMonth = (date.getMillis - Epoch.plus(months).getMillis) / 1000L
    BinnedTime(months.getMonths.toShort, secondsInMonth)
  }

  private def fromMonthAndSeconds(date: BinnedTime): DateTime =
    Epoch.plusMonths(date.bin).plus(date.offset * 1000L)

  private def toYearAndMinutes(time: Long): BinnedTime =
    toYearAndMinutes(new DateTime(time, DateTimeZone.UTC))

  private def toYearAndMinutes(date: DateTime): BinnedTime = {
    require(YearsMaxDate.isAfter(date), s"Date exceeds maximum indexable value ($YearsMaxDate): $date")
    val years = Years.yearsBetween(Epoch, date)
    val minutesInYear = (date.getMillis - Epoch.plus(years).getMillis) / 60000L
    BinnedTime(years.getYears.toShort, minutesInYear)
  }

  private def fromYearAndMinutes(date: BinnedTime): DateTime =
    Epoch.plusYears(date.bin).plus(date.offset * 60000L)
}

object TimePeriod extends Enumeration {

  type TimePeriod = Value

  val Day   = Value("day")
  val Week  = Value("week")
  val Month = Value("month")
  val Year  = Value("year")
}
