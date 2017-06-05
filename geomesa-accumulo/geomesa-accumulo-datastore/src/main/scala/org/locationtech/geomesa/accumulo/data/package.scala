/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.data.Value
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.features.SerializationType

package object data {

  // Storage implementation constants
  val EMPTY_STRING         = ""
  val EMPTY_VALUE          = new Value(Array[Byte]())
  val EMPTY_COLF           = new Text(EMPTY_STRING)
  val EMPTY_COLQ           = new Text(EMPTY_STRING)
  val EMPTY_VIZ            = new Text(EMPTY_STRING)
  val EMPTY_TEXT           = new Text()
  val DEFAULT_ENCODING     = SerializationType.KRYO
}
