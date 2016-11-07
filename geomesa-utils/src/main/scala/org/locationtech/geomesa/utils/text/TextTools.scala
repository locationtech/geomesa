package org.locationtech.geomesa.utils.text

import org.joda.time.Period
import org.joda.time.format.PeriodFormatterBuilder

object TextTools {
  val PeriodFormatter =
    new PeriodFormatterBuilder().minimumPrintedDigits(2).printZeroAlways()
      .appendHours().appendSeparator(":").appendMinutes().appendSeparator(":").appendSeconds().toFormatter

  def getPlural(i: Long, base: String): String = if (i == 1) s"$i $base" else s"$i ${base}s"

  /**
   * Gets elapsed time as a string
   */
  def getTime(start: Long): String = PeriodFormatter.print(new Period(System.currentTimeMillis() - start))

  /**
   * Gets status as a string
   */
  def getStatInfo(successes: Long, failures: Long): String = {
    val failureString = if (failures == 0) {
      "with no failures"
    } else {
      s"and failed to ingest ${getPlural(failures, "feature")}"
    }
    s"Ingested ${getPlural(successes, "feature")} $failureString."
  }

  def buildString(c: Char, length: Int): String = {
    if (length < 0) return ""
    val sb = new StringBuilder(length)
    (0 until length).foreach(_ => sb.append(c))
    sb.toString()
  }
}
