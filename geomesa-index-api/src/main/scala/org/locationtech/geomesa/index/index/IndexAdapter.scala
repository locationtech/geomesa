/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

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
}
