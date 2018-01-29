/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import java.time._
import java.time.temporal.ChronoUnit

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
  type DateToBinnedTime = (ZonedDateTime) => BinnedTime
  type BinnedTimeToDate = (BinnedTime) => ZonedDateTime

  val Epoch: ZonedDateTime = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)

  // min value (inclusive)
  val ZMinDate: ZonedDateTime = Epoch

  // max values (exclusive)
  val DaysMaxDate  : ZonedDateTime = Epoch.plusDays(Short.MaxValue.toInt + 1)
  val WeeksMaxDate : ZonedDateTime = Epoch.plusWeeks(Short.MaxValue.toInt + 1)
  val MonthsMaxDate: ZonedDateTime = Epoch.plusMonths(Short.MaxValue.toInt + 1)
  val YearsMaxDate : ZonedDateTime = Epoch.plusYears(Short.MaxValue.toInt + 1)

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
      case TimePeriod.Day   => ChronoUnit.DAYS.getDuration.toMillis
      case TimePeriod.Week  => ChronoUnit.WEEKS.getDuration.toMillis / 1000L
      case TimePeriod.Month => (ChronoUnit.DAYS.getDuration.toMillis / 1000L) * 31L
      case TimePeriod.Year  => ChronoUnit.WEEKS.getDuration.toMinutes * 52L
    }
  }

  /**
    * Max indexable date (exclusive) for a given time period
    *
    * @param period interval type
    * @return
    */
  def maxDate(period: TimePeriod): ZonedDateTime = {
    period match {
      case TimePeriod.Day   => DaysMaxDate
      case TimePeriod.Week  => WeeksMaxDate
      case TimePeriod.Month => MonthsMaxDate
      case TimePeriod.Year  => YearsMaxDate
    }
  }

  /**
    * Converts values extracted from a filter into valid indexable bounds
    *
    * @param period time period
    * @return
    */
  def boundsToIndexableDates(period: TimePeriod): ((Option[ZonedDateTime], Option[ZonedDateTime])) => (ZonedDateTime, ZonedDateTime) = {
    val maxDateTime = maxDate(period).minus(1L, ChronoUnit.MILLIS)
    (bounds) => {
      val lo = bounds._1 match {
        case None => ZMinDate
        case Some(dt) if dt.isBefore(ZMinDate) => ZMinDate
        case Some(dt) if dt.isAfter(maxDateTime) => maxDateTime
        case Some(dt) => dt
      }
      val hi = bounds._2 match {
        case None => maxDateTime
        case Some(dt) if dt.isBefore(ZMinDate) => ZMinDate
        case Some(dt) if dt.isAfter(maxDateTime) => maxDateTime
        case Some(dt) => dt
      }
      (lo, hi)
    }
  }

  private def toDayAndMillis(time: Long): BinnedTime =
    toDayAndMillis(ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC))

  private def toDayAndMillis(date: ZonedDateTime): BinnedTime = {
    require(!date.isBefore(ZMinDate), s"Date exceeds minimum indexable value ($ZMinDate): $date")
    require(DaysMaxDate.isAfter(date), s"Date exceeds maximum indexable value ($DaysMaxDate): $date")
    val days = ChronoUnit.DAYS.between(Epoch, date)
    val millisInDay = date.toInstant.toEpochMilli - Epoch.plus(days, ChronoUnit.DAYS).toInstant.toEpochMilli
    BinnedTime(days.toShort, millisInDay)
  }

  private def fromDayAndMillis(date: BinnedTime): ZonedDateTime =
    Epoch.plusDays(date.bin).plus(date.offset, ChronoUnit.MILLIS)

  private def toWeekAndSeconds(time: Long): BinnedTime =
    toWeekAndSeconds(ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC))

  private def toWeekAndSeconds(date: ZonedDateTime): BinnedTime = {
    require(!date.isBefore(ZMinDate), s"Date exceeds minimum indexable value ($ZMinDate): $date")
    require(WeeksMaxDate.isAfter(date), s"Date exceeds maximum indexable value ($WeeksMaxDate): $date")
    val weeks = ChronoUnit.WEEKS.between(Epoch, date)
    val secondsInWeek = date.toEpochSecond - Epoch.plus(weeks, ChronoUnit.WEEKS).toEpochSecond
    BinnedTime(weeks.toShort, secondsInWeek)
  }

  private def fromWeekAndSeconds(date: BinnedTime): ZonedDateTime =
    Epoch.plusWeeks(date.bin).plus(date.offset, ChronoUnit.SECONDS)

  private def toMonthAndSeconds(time: Long): BinnedTime =
    toMonthAndSeconds(ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC))

  private def toMonthAndSeconds(date: ZonedDateTime): BinnedTime = {
    require(!date.isBefore(ZMinDate), s"Date exceeds minimum indexable value ($ZMinDate): $date")
    require(MonthsMaxDate.isAfter(date), s"Date exceeds maximum indexable value ($MonthsMaxDate): $date")
    val months = ChronoUnit.MONTHS.between(Epoch, date)
    val secondsInMonth = date.toEpochSecond - Epoch.plus(months, ChronoUnit.MONTHS).toEpochSecond
    BinnedTime(months.toShort, secondsInMonth)
  }

  private def fromMonthAndSeconds(date: BinnedTime): ZonedDateTime =
    Epoch.plusMonths(date.bin).plus(date.offset, ChronoUnit.SECONDS)

  private def toYearAndMinutes(time: Long): BinnedTime =
    toYearAndMinutes(ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC))

  private def toYearAndMinutes(date: ZonedDateTime): BinnedTime = {
    require(!date.isBefore(ZMinDate), s"Date exceeds minimum indexable value ($ZMinDate): $date")
    require(YearsMaxDate.isAfter(date), s"Date exceeds maximum indexable value ($YearsMaxDate): $date")
    val years = ChronoUnit.YEARS.between(Epoch, date)
    val minutesInYear = (date.toEpochSecond - Epoch.plus(years, ChronoUnit.YEARS).toEpochSecond) / 60L
    BinnedTime(years.toShort, minutesInYear)
  }

  private def fromYearAndMinutes(date: BinnedTime): ZonedDateTime =
    Epoch.plusYears(date.bin).plus(date.offset, ChronoUnit.MINUTES)
}

object TimePeriod extends Enumeration {

  type TimePeriod = Value

  val Day:   Value = Value("day")
  val Week:  Value = Value("week")
  val Month: Value = Value("month")
  val Year:  Value = Value("year")
}
