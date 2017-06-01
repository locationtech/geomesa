/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import org.joda.time.Period
import org.joda.time.format.PeriodFormatterBuilder

object TextTools {
  val PeriodFormatter =
    new PeriodFormatterBuilder().minimumPrintedDigits(2).printZeroAlways()
      .appendHours().appendSeparator(":").appendMinutes().appendSeparator(":").appendSeconds().toFormatter

  def getPlural(i: Long, base: String): String = getPlural(i, base, s"${base}s")

  def getPlural(i: Long, base: String, pluralBase: String): String = if (i == 1) s"$i $base" else s"$i ${pluralBase}"

  /**
   * Gets elapsed time as a string
   */
  def getTime(start: Long): String = PeriodFormatter.print(new Period(System.currentTimeMillis() - start))

  def buildString(c: Char, length: Int): String = {
    if (length < 0) ""
    else {
      val sb = new StringBuilder(length)
      (0 until length).foreach(_ => sb.append(c))
      sb.toString()
    }
  }
}
