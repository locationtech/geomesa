/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

object ErrorMode extends Enumeration with Modes {
  type ErrorMode = Value
  val SkipBadRecords: ErrorMode = Value("skip-bad-records")
  val RaiseErrors   : ErrorMode = Value("raise-errors")
  val Default       : ErrorMode = SkipBadRecords

  def apply(): ErrorMode = apply("converter.error.mode", SkipBadRecords)
}

object ParseMode extends Enumeration {
  type ParseMode = Value
  val Incremental: ParseMode = Value("incremental")
  val Batch      : ParseMode = Value("batch")
  val Default    : ParseMode = Incremental
}


object LineMode extends Enumeration {
  type LineMode = Value
  val Single : LineMode = Value("single")
  val Multi  : LineMode = Value("multi")
  val Default: LineMode = Single
}

trait Modes {
  this: Enumeration =>

  def apply[T](propertyName: String, defaultValue: T): T = {
    val string = SystemProperty(propertyName, defaultValue.toString)
    this.values.find(_.toString.equalsIgnoreCase(string.get)) match {
      case Some(v)=> v.asInstanceOf[T]
      case None => defaultValue
    }
  }
}

