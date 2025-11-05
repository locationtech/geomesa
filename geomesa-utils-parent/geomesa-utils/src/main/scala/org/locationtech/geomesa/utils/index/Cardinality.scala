/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

object Cardinality extends Enumeration {
  type Cardinality = Value
  val HIGH: Value    = Value("high")
  val LOW: Value     = Value("low")
  val UNKNOWN: Value = Value("unknown")
}
