/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

object ErrorMode extends Enumeration {
  type ErrorMode = Value
  val SkipBadRecords: ErrorMode = Value("skip-bad-records")
  val RaiseErrors   : ErrorMode = Value("raise-errors")
  val Default       : ErrorMode = SkipBadRecords

  def apply(): ErrorMode = {
    val errorMode = SystemProperty("converter.error.mode", SkipBadRecords.toString)
    ErrorMode.values.find(_.toString.equalsIgnoreCase(errorMode.get)) match {
      case Some(v)=> v.asInstanceOf[ErrorMode]
      case None => SkipBadRecords
    }
  }
}
