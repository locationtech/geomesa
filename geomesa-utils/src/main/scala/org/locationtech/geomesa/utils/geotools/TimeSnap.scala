/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.joda.time.{DateTime, Duration, Interval}

class TimeSnap(val interval: Interval, val buckets: Int) {

  // convert to milliseconds and find the step size in milliseconds
  val dt: Duration = new Duration(interval.toDurationMillis / buckets)

  /**
   * Computes the time ordinate of the i'th grid column.
   * @param i the index of a grid column
   * @return the t time ordinate of the column
   */
  def t(i: Int): DateTime = i match {
    case x if x >= buckets => interval.getEnd
    case x if x < 0 => interval.getStart
    case x => interval.getStart.plus(dt.getMillis * i)
  }

  /**
   * Computes the column index of a time ordinate.
   * @param t the time ordinate
   * @return the column index
   */
  def i(t: DateTime): Int = t match {
    case x if x.isAfter(interval.getEnd) => buckets
    case x if x.isBefore(interval.getStart) => -1
    case x =>
      val ret = (x.getMillis - interval.getStart.getMillis) / dt.getMillis
      if (ret >= buckets) buckets - 1
      else ret.toInt
  }
}