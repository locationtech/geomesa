/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.joda.time.{DateTime, Seconds, Weeks}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BinnedTimeTest extends Specification {

  val rand = new Random(-574)

  def nextTime =
    BinnedTime.Epoch
        .plusYears(rand.nextInt(40))
        .plusMonths(rand.nextInt(12))
        .plusDays(rand.nextInt(28))
        .plusHours(rand.nextInt(24))
        .plusMinutes(rand.nextInt(60))
        .plusSeconds(rand.nextInt(60))
        .plusMillis(rand.nextInt(1000))

  def getWeekAndSeconds_old(time: DateTime): (Short, Int) = {
    val weeks = Weeks.weeksBetween(BinnedTime.Epoch, time)
    val secondsInWeek = Seconds.secondsBetween(BinnedTime.Epoch, time).getSeconds - weeks.toStandardSeconds.getSeconds
    (weeks.getWeeks.toShort, secondsInWeek)
  }

  "BinnedTime" should {

    "convert to weeks and seconds and back" >> {
      forall((0 until 10).map(_ => nextTime)) { time =>
        val binned = BinnedTime.dateToBinnedTime(TimePeriod.Week)(time)
        BinnedTime.binnedTimeToDate(TimePeriod.Week)(binned) mustEqual time.minusMillis(time.getMillisOfSecond)
      }
    }

    "convert to days and millis and back" >> {
      forall((0 until 10).map(_ => nextTime)) { time =>
        val binned = BinnedTime.dateToBinnedTime(TimePeriod.Day)(time)
        BinnedTime.binnedTimeToDate(TimePeriod.Day)(binned) mustEqual time
      }
    }

    "convert to months and seconds and back" >> {
      forall((0 until 10).map(_ => nextTime)) { time =>
        val binned = BinnedTime.dateToBinnedTime(TimePeriod.Month)(time)
        BinnedTime.binnedTimeToDate(TimePeriod.Month)(binned) mustEqual time.minusMillis(time.getMillisOfSecond)
      }
    }

    "convert to years and minutes and back" >> {
      forall((0 until 10).map(_ => nextTime)) { time =>
        val binned = BinnedTime.dateToBinnedTime(TimePeriod.Year)(time)
        BinnedTime.binnedTimeToDate(TimePeriod.Year)(binned) mustEqual
            time.minusMillis(time.getMillisOfSecond).minusSeconds(time.getSecondOfMinute)
      }
    }

    "convert to weeks and seconds back compatible" >> {
      forall((0 until 10).map(_ => nextTime)) { time =>
        val toBin = BinnedTime.dateToBinnedTime(TimePeriod.Week)
        val BinnedTime(weeks, seconds) = toBin(time)
        getWeekAndSeconds_old(time) mustEqual (weeks, seconds)
      }
    }
  }
}
