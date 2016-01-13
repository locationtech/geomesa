/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.convert

import com.typesafe.config.Config

object LineMode {
  val Single  = "single"
  val Multi   = "multi"
  val Default = Single

  def getLineMode(conf: Config): String = {
    if (conf.hasPath(StandardOptions.LineMode)) {
      val m = conf.getString(StandardOptions.LineMode)
      LineMode.check(m)
      m
    } else {
      LineMode.Default
    }
  }

  def check(s: String) =
    if (! Seq(Single, Multi).contains(s)) {
      throw new IllegalArgumentException(s"Invalid line mode: $s valid values are $Single and $Multi")
    }
}
