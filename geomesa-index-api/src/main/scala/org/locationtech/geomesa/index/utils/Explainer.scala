/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.io.PrintStream

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

trait Explainer {
  private var indent = ""
  def apply(s: => String): Explainer = { output(s"$indent$s"); this }
  def apply(s: => String, c: Seq[() => String]): Explainer = {
    output(s"$indent$s")
    pushLevel()
    c.foreach(s => output(s"$indent${s.apply}"))
    popLevel()
  }
  def pushLevel(): Explainer = { indent += "  "; this }
  def pushLevel(s: => String): Explainer = { apply(s); pushLevel(); this }
  def popLevel(): Explainer = { indent = indent.substring(2); this }
  def popLevel(s: => String): Explainer = { popLevel(); apply(s); this }
  protected def output(s: => String)
}

class ExplainPrintln(out: PrintStream = System.out) extends Explainer {
  override def output(s: => String): Unit = out.println(s)
}

object ExplainNull extends Explainer {
  override def apply(s: => String): Explainer = this
  override def apply(s: => String, c: Seq[() => String]): Explainer = this
  override def pushLevel(): Explainer = this
  override def pushLevel(s: => String): Explainer = this
  override def popLevel(): Explainer = this
  override def popLevel(s: => String): Explainer = this
  override def output(s: => String): Unit = {}
}

class ExplainString extends Explainer {
  private val string: StringBuilder = new StringBuilder()
  override def output(s: => String): Unit = string.append(s).append("\n")
  override def toString: String = string.toString()
}

class ExplainLogger(logger: Logger) extends Explainer {
  override def output(s: => String): Unit = logger.trace(s)
}

class ExplainLogging extends ExplainLogger(ExplainLogging.logger)

object ExplainLogging {
  private val logger = Logger(LoggerFactory.getLogger(classOf[Explainer]))
}
