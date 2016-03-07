/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamo.core

import org.joda.time.{Seconds, Weeks, DateTime}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.sfcurve.zorder.ZCurve2D

trait DynamoPrimaryKey {

  case class Key(idx: Int, x: Double, y: Double, dk: Int, z: Int)

  val SFC3D = new Z3SFC
  val SFC2D = new ZCurve2D(math.pow(2,5).toInt)

  val EPOCH = new DateTime(0)
  val ONE_WEEK_IN_SECONDS = Weeks.ONE.toStandardSeconds.getSeconds

  def epochWeeks(dtg: DateTime): Weeks = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime): Int =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - epochWeeks(dtg).getWeeks*ONE_WEEK_IN_SECONDS

  def unapply(idx: Int): Key = {
    val dk = idx >> 16
    val z = idx & 0x000000ff
    val (x, y) = SFC2D.toPoint(z)
    Key(idx, x, y, dk, z)
  }

  def apply(dtg: DateTime, x: Double, y: Double): Key = {
    val dk = epochWeeks(dtg).getWeeks << 16
    val z = SFC2D.toIndex(x, y).toInt
    val (rx, ry) = SFC2D.toPoint(z)
    val idx = dk + z
    Key(idx, rx, ry, dk, z)
  }

}
