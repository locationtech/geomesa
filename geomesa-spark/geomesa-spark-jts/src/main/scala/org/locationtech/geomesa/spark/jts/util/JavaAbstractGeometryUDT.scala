/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.util

import org.locationtech.jts.geom.Geometry

class JavaAbstractGeometryUDT {
  def deserializer(datum: Array[Byte]): Geometry = {
    WKBUtils.read(datum)
  }
}

object JavaAbstractGeometryUDT {
  val jagu = new JavaAbstractGeometryUDT()
  def deserialize(datum: Array[Byte]): Geometry = jagu.deserializer(datum)
}
