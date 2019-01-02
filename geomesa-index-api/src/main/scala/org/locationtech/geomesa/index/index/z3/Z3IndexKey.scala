/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3

case class Z3IndexKey(bin: Short, z: Long) extends Ordered[Z3IndexKey] {
  override def compare(that: Z3IndexKey): Int = {
    val b = Ordering.Short.compare(bin, that.bin)
    if (b != 0) { b } else {
      Ordering.Long.compare(z, that.z)
    }
  }
}
