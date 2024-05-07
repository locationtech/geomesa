/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

trait Modes {

  this: Enumeration =>

  type Mode

  protected def defaultValue: Mode

  def systemProperty: SystemProperty

  def apply(): Mode = {
    val string = systemProperty.get
    values.find(_.toString.equalsIgnoreCase(string)).map(_.asInstanceOf[Mode]).getOrElse(defaultValue)
  }
}

object Modes {

  type ErrorMode = ErrorMode.Value
  type ParseMode = ParseMode.Value
  type LineMode  = LineMode.Value

  object ErrorMode extends Enumeration with Modes {

    type Mode = Modes.ErrorMode
    val RaiseErrors   : ErrorMode = Value("raise-errors")
    val LogErrors     : ErrorMode = Value("log-errors")
    val ReturnErrors  : ErrorMode = Value("return-errors")
    def Default       : ErrorMode = apply()

    def apply(mode: String): ErrorMode = {
      values.find(_.toString.equalsIgnoreCase(mode)) match {
        case Some(m) => m
        case None =>
          if ("skip-bad-records".equalsIgnoreCase(mode)) {
            LogErrors
          } else {
            throw new IllegalArgumentException(
              s"Invalid error mode '$mode'. Valid values are ${values.mkString("'", "', '", "'")}")
          }
      }
    }

    override protected val defaultValue: ErrorMode = LogErrors
    override val systemProperty: SystemProperty =
      SystemProperty("geomesa.converter.error.mode.default", defaultValue.toString)
  }

  object ParseMode extends Enumeration with Modes {
    type Mode = Modes.ParseMode
    val Incremental: ParseMode = Value("incremental")
    val Batch      : ParseMode = Value("batch")
    def Default    : ParseMode = apply()

    override protected val defaultValue: ParseMode = Incremental
    override val systemProperty: SystemProperty =
      SystemProperty("geomesa.converter.parse.mode.default", defaultValue.toString)
  }

  object LineMode extends Enumeration with Modes {
    type Mode = Modes.LineMode
    val Single : LineMode = Value("single")
    val Multi  : LineMode = Value("multi")
    def Default: LineMode = apply()

    override protected val defaultValue: LineMode = Single
    override val systemProperty: SystemProperty =
      SystemProperty("geomesa.converter.line.mode.default", defaultValue.toString)
  }
}
