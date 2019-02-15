/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.PrintStream

import com.beust.jcommander.ParameterException
import org.geotools.data.{DataStore, DataStoreFinder, DataUtilities}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.AbstractConverterIngest.{PrintProgress, StatusCallback}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.TextTools
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

abstract class AbstractConverterIngest(dsParams: Map[String, String], sft: SimpleFeatureType) extends Runnable {

  import scala.collection.JavaConverters._

  override def run(): Unit = {
    // create schema for the feature prior to ingest
    val ds = DataStoreFinder.getDataStore(dsParams.asJava)
    try {
      val existing = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
      if (existing == null) {
        Command.user.info(s"Creating schema '${sft.getTypeName}'")
        ds.createSchema(sft)
      } else {
        Command.user.info(s"Schema '${sft.getTypeName}' exists")
        if (DataUtilities.compare(sft, existing) != 0) {
          throw new ParameterException("Existing simple feature type does not match expected type" +
              s"\n  existing: '${SimpleFeatureTypes.encodeType(existing)}'" +
              s"\n  expected: '${SimpleFeatureTypes.encodeType(sft)}'")
        }
      }
      runIngest(ds, sft, createCallback())
    } finally {
      ds.dispose()
    }
  }

  protected def runIngest(ds: DataStore, sft: SimpleFeatureType, callback: StatusCallback): Unit

  private def createCallback(): StatusCallback = {
    if (Seq("accumulo.mock", "useMock").exists(dsParams.get(_).exists(_.toBoolean))) {
      new PrintProgress(System.err, TextTools.buildString('\u26AC', 60), ' ', '\u15e7', '\u2b58')
    } else {
      new PrintProgress(System.err, TextTools.buildString(' ', 60), '\u003d', '\u003e', '\u003e')
    }
  }
}

object AbstractConverterIngest {

  lazy private val terminalWidth: () => Float = {
    val jline = for {
      terminalClass <- Try(Class.forName("jline.Terminal"))
      terminal      <- Try(terminalClass.getMethod("getTerminal").invoke(null))
      method        <- Try(terminalClass.getMethod("getTerminalWidth"))
    } yield {
      () => method.invoke(terminal).asInstanceOf[Int].toFloat
    }
    jline.getOrElse(() => 1.0f)
  }

  sealed trait StatusCallback {
    def reset(): Unit
    def apply(prefix: String, progress: Float, counters: Seq[(String, Long)], done: Boolean): Unit
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

      // Figure out if and how much the progress bar should be scaled to accommodate smaller terminals
      val scaleFactor: Float = {
        val tWidth = terminalWidth()
        // Sanity check as jline may not be correct. We also don't scale up, ~112 := scaleFactor = 1.0f
        if (tWidth > info.length + 3 && tWidth < emptyBar.length + info.length + 2 + prefix.length) {
          // Screen Width 80 yields scaleFactor of .46
          (tWidth - info.length - 2 - prefix.length) / emptyBar.length // -2 is for brackets around bar
        } else {
          1.0f
        }
      }

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
