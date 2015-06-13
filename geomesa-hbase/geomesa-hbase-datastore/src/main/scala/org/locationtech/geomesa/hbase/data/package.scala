package org.locationtech.geomesa.hbase

import org.joda.time.{DateTime, Seconds, Weeks}

package object data {
  val EPOCH = new DateTime(0)

  def epochWeeks(dtg: DateTime) = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

}
