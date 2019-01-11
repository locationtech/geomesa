/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.util.concurrent.ConcurrentHashMap


object SplitArrays {

  val EmptySplits = IndexedSeq(Array.empty[Byte])

  private val splitArraysMap: ConcurrentHashMap[Int, IndexedSeq[Array[Byte]]] =
    new ConcurrentHashMap[Int, IndexedSeq[Array[Byte]]]()

  def apply(numSplits: Int): IndexedSeq[Array[Byte]] = {
    if (numSplits < 2) { EmptySplits } else {
      var splits = splitArraysMap.get(numSplits)
      if (splits == null) {
        splits = (0 until numSplits).map(_.toByte).toArray.map(Array(_)).toIndexedSeq
        splitArraysMap.put(numSplits, splits)
      }
      splits
    }
  }
}
