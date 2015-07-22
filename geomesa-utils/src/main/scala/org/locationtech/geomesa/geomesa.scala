/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech

package object geomesa {

  // 0 == old single table style
  // 1 == multi-table style
  // 2 == sorted keys in the STIDX table
  // 3 == skipped for integration
  // 4 == kryo encoded index values
  // 5 == z3 index
  // 6 == attribute indices with dates
  val CURRENT_SCHEMA_VERSION = 6
}
