/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.util.concurrent.ConcurrentHashMap

import org.opengis.feature.simple.SimpleFeatureType


object SplitArrays {

  private val splitArraysMap: ConcurrentHashMap[Int, IndexedSeq[Array[Byte]]] =
    new ConcurrentHashMap[Int, IndexedSeq[Array[Byte]]]()

  def apply(sft: SimpleFeatureType): IndexedSeq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    apply(sft.getZShards)
  }

  def apply(numSplits: Int): IndexedSeq[Array[Byte]] = {
    val temp = splitArraysMap.get(numSplits)
    if (temp == null) {
      val splitArrays = (0 until numSplits).map(_.toByte).toArray.map(Array(_)).toIndexedSeq
      splitArraysMap.put(numSplits, splitArrays)
      splitArrays
    } else {
      temp
    }
  }
}
