/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech

package object geomesa {

  // 0  -> old single table style
  // 1  -> multi-table style
  // 2  -> sorted keys in the STIDX table
  // 3  -> skipped for integration
  // 4  -> kryo encoded index values
  // 5  -> z3 index (1.1.0-rc.1 through 1.1.0-rc.2)
  // 6  -> attribute indices with dates (1.1.0-rc.3 through 1.2.0)
  // 7  -> z3 polygons (1.2.1)
  // 8  -> z2 index, deprecating stidx (1.2.2 through 1.2.4)
  // 9  -> per-attribute-visibilities, not serializing ID in value (not released separately)
  // 10 -> XZ2, XZ3 (1.2.5+)
  val CURRENT_SCHEMA_VERSION = 10
}
