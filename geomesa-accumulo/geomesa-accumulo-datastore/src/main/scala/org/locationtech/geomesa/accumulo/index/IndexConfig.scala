/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

trait IndexConfig {
  val numSplits: Int
  val splitArrays: Seq[Array[Byte]]
}

case object DefaultIndexConfig extends IndexConfig {
  val numSplits = 4
  val splitArrays = (0 until numSplits).map(_.toByte).toArray.map(Array(_)).toSeq
}
