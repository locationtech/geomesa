/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conversions

object StringOps {

  implicit class RichString(val string: String) extends AnyVal {

    /**
     * For every line in this string:
     *
     * Strip a leading prefix consisting of blanks or control characters,
     * followed by separator from the line,
     * followed by any whitespace
     *
     * @param separator line separator
     * @return
     */
    def stripMarginAndWhitespace(separator: Char = '|'): String = {
      val trimmed = new StringBuilder()
      string.linesWithSeparators.foreach { line =>
        var index = line.indexWhere(_ > ' ')
        if (index == -1) {
          trimmed.append(line)
        } else {
          if (line.charAt(index) == separator) {
            index += 1
          }
          while (index < line.length && line.charAt(index).isWhitespace) {
            index += 1
          }
          if (index < line.length) {
            trimmed.append(line.substring(index))
          }
        }
      }
      trimmed.toString
    }
  }
}
