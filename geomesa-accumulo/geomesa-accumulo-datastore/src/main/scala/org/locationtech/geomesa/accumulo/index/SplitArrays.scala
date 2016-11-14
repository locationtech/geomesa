/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index



object SplitArrays {
  val splitArraysMap: scala.collection.mutable.HashMap[Int, Seq[Array[Byte]]] =
    new scala.collection.mutable.HashMap[Int, Seq[Array[Byte]]]()

  def getSplitArray(numSplits: Int): Seq[Array[Byte]] =
    splitArraysMap.getOrElseUpdate(numSplits, (0 until numSplits).map(_.toByte).toArray.map(Array(_)).toSeq)
}
