/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import java.time.Duration

object TextTools {

  def getPlural(i: Long, base: String): String = getPlural(i, base, s"${base}s")

  def getPlural(i: Long, base: String, pluralBase: String): String = if (i == 1) s"$i $base" else s"$i $pluralBase"

  /**
   * Gets elapsed time as a string
   */
  def getTime(start: Long): String = {
    val duration = Duration.ofMillis(System.currentTimeMillis() - start)
    val hours = duration.toHours
    val minusHours = duration.minusHours(hours)
    val minutes = minusHours.toMinutes
    val seconds = minusHours.minusMinutes(minutes).getSeconds
    f"$hours%02d:$minutes%02d:$seconds%02d"
  }

  def buildString(c: Char, length: Int): String = {
    if (length < 0) { "" } else {
      new String(Array.fill(length)(c))
    }
  }
}
