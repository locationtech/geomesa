/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import com.typesafe.config.Config

object LineMode extends Enumeration {
  type LineMode = Value

  val Single  = Value("single")
  val Multi   = Value("multi")

  val Default = Single

  def getLineMode(conf: Config): LineMode = {
    if (conf.hasPath(StandardOptions.LineMode)) {
      val m = conf.getString(StandardOptions.LineMode).toLowerCase
      LineMode.withName(m)
    } else {
      LineMode.Default
    }
  }

}
