/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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

  /**
    * Builds a natural word list, e.g. 'foo, bar, baz and blu'
    *
    * @param words words
    * @return
    */
  def wordList(words: Iterable[String]): String = {
    if (words.isEmpty) { "" } else {
      val iter = words.iterator
      var word: String = iter.next
      if (iter.hasNext) {
        val builder = new StringBuilder(word)
        word = iter.next
        while (iter.hasNext) {
          builder.append(", ").append(word)
          word = iter.next
        }
        builder.append(" and ").append(word)
        builder.result()
      } else {
        word
      }
    }
  }

  /**
    * Checks if a string contains non-whitespace
    *
    * @param string string, not null
    * @return
    */
  def isWhitespace(string: String): Boolean = {
    var i = 0
    while (i < string.length) {
      if (!string.charAt(i).isWhitespace) {
        return false
      }
      i += 1
    }
    true
  }
}
