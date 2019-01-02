/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import org.locationtech.geomesa.convert.Modes.ParseMode.Incremental
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

trait Modes {
  this: Enumeration =>

  type Mode
  protected def defaultValue: Mode
  def systemProperty: SystemProperty

  def apply(): Mode = {
    val string = systemProperty.get
    this.values.find(_.toString.equalsIgnoreCase(string)) match {
      case Some(v)=> v.asInstanceOf[Mode]
      case None => defaultValue
    }
  }
}

object Modes {
  type ErrorMode = ErrorMode.Value
  type ParseMode = ParseMode.Value
  type LineMode  = LineMode.Value

  object ErrorMode extends Enumeration with Modes {
    type Mode = Modes.ErrorMode
    val SkipBadRecords: ErrorMode = Value("skip-bad-records")
    val RaiseErrors   : ErrorMode = Value("raise-errors")
    def Default       : ErrorMode = apply()

    override protected val defaultValue: ErrorMode = SkipBadRecords
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
