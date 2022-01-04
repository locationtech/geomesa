/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import java.io.PrintStream

import org.locationtech.geomesa.jobs.StatusCallback
import org.locationtech.geomesa.utils.text.TextTools

object TerminalCallback {

  @deprecated("JLine breaks tty settings when invoked")
  lazy val terminalWidth: () => Float = () => 1.0f

  def apply(mock: Boolean = false): StatusCallback = {
    if (mock) {
      new PrintProgress(System.err, TextTools.buildString('\u26AC', 60), ' ', '\u15e7', '\u2b58')
    } else {
      new PrintProgress(System.err, TextTools.buildString(' ', 60), '\u003d', '\u003e', '\u003e')
    }
  }

  /**
    * Prints progress using the provided output stream. Progress will be overwritten using '\r', and will only
    * include a line feed if done == true
    */
  final class PrintProgress(out: PrintStream, emptyBar: String, replacement: Char, indicator: Char, toggle: Char)
      extends StatusCallback {

    private var toggled = false
    private var start = System.currentTimeMillis()

    override def reset(): Unit = start = System.currentTimeMillis()

    override def apply(prefix: String, progress: Float, counters: Seq[(String, Long)], done: Boolean): Unit = {
      val percent = f"${(progress * 100).toInt}%3d"
      val counterString = if (counters.isEmpty) { "" } else {
        counters.map { case (label, count) => s"$count $label"}.mkString(" ", " ", "")
      }
      val info = s" $percent% complete$counterString in ${TextTools.getTime(start)}"

      // scale factor for the progress bar to accommodate smaller terminals
      // no longer used due to bugs in jline, but logic left in case we want to resurrect it
      val scaleFactor: Float = 1.0f

      val scaledLen = (emptyBar.length * scaleFactor).toInt
      val numDone = (scaledLen * progress).toInt
      val bar = if (numDone < 1) {
        emptyBar.substring(emptyBar.length - scaledLen)
      } else if (numDone >= scaledLen) {
        TextTools.buildString(replacement, scaledLen)
      } else {
        val doneStr = TextTools.buildString(replacement, numDone - 1) // -1 for indicator
        val doStr = emptyBar.substring(emptyBar.length - (scaledLen - numDone))
        val i = if (toggled) { toggle } else { indicator }
        toggled = !toggled
        s"$doneStr$i$doStr"
      }

      // use \r to replace current line
      // trailing space separates cursor
      out.print(s"\r$prefix[$bar]$info")
      if (done) {
        out.println()
      }
    }
  }
}
