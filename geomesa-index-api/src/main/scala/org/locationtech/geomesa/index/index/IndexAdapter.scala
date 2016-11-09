/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

/**
* Portions
* Copyright 2011-2016 The Apache Software Foundation
*/

package org.locationtech.geomesa.index.index

import org.geotools.factory.Hints
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait IndexAdapter[DS <: GeoMesaDataStore[DS, F, W, Q], F <: WrappedFeature, W, Q, R] {

  def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String

  protected def entriesToFeatures(sft: SimpleFeatureType, returnSft: SimpleFeatureType): (Q) => SimpleFeature
  protected def createInsert(row: Array[Byte], feature: F): W
  protected def createDelete(row: Array[Byte], feature: F): W

  // range with start row included and end row excluded
  protected def range(start: Array[Byte], end: Array[Byte]): R
  protected def rangeExact(row: Array[Byte]): R
  protected def rangePrefix(prefix: Array[Byte]): R

  protected def scanPlan(sft: SimpleFeatureType,
                         ds: DS,
                         filter: FilterStrategy[DS, F, W, Q],
                         hints: Hints,
                         ranges: Seq[R],
                         ecql: Option[Filter]): QueryPlan[DS, F, W, Q]
}

object IndexAdapter {

  val DefaultNumSplits = 4 // can't be more than Byte.MaxValue (127)
  val DefaultSplitArrays = (0 until DefaultNumSplits).map(_.toByte).toArray.map(Array(_)).toSeq

  /**
    * Returns a row that sorts just after all rows beginning with a prefix. Copied from Accumulo Range
    *
    * @param prefix to follow
    * @return prefix that immediately follows the given prefix when sorted, or null if no prefix can follow
    *         (i.e., the string is all 0xff bytes)
    */
  def followingPrefix(prefix: Array[Byte]): Array[Byte] = {
    // find the last byte in the array that is not 0xff
    var changeIndex = prefix.length - 1
    while (changeIndex >= 0 && prefix(changeIndex) == 0xff.toByte) {
      changeIndex -= 1
    }
    if (changeIndex < 0) { null } else {
      // copy prefix bytes into new array
      val following = Array.ofDim[Byte](changeIndex + 1)
      System.arraycopy(prefix, 0, following, 0, changeIndex + 1)
      // increment the selected byte
      following(changeIndex) = (following(changeIndex) + 1).toByte
      following
    }
  }

  /**
    * Returns a row that immediately follows the row. Useful for inclusive endpoints.
    *
    * @param row row
    * @return
    */
  def followingRow(row: Array[Byte]): Array[Byte] = {
    val following = Array.ofDim[Byte](row.length + 1)
    System.arraycopy(row, 0, following, 0, row.length)
    following(row.length) = 0x00.toByte
    following
  }
}
