/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.concurrent.ConcurrentHashMap


object SplitArrays {
  val splitArraysMap: ConcurrentHashMap[Int, Seq[Array[Byte]]] =
    new ConcurrentHashMap[Int, Seq[Array[Byte]]]()

  def getSplitArray(numSplits: Int): Seq[Array[Byte]] = {
    require(numSplits > 0 && numSplits < 128, "only up to 128 splits are supported")
    val temp = splitArraysMap.get(numSplits)
    if (temp == null) splitArraysMap.put(numSplits, (0 until numSplits).map(_.toByte).toArray.map(Array(_)).toSeq)
    splitArraysMap.get(numSplits)
  }
}
