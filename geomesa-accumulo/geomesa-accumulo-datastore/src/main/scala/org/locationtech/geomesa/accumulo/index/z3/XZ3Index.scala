/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z3

import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.index.SplitArrays
import org.opengis.feature.simple.SimpleFeatureType

/*case class XZ3IndexConfig(numSplits: Int = XZ3Index.numSplits) extends IndexConfig {
  val splitArrays = (0 until numSplits).map(_.toByte).toArray.map(Array(_)).toSeq
}

case class XZ3Index(conf: IndexConfig)
  extends AccumuloFeatureIndexType with IndexConfig with XZ3WritableIndex with XZ3QueryableIndex {

  val numSplits = conf.numSplits
  val splitArrays = conf.splitArrays

  override val name: String = XZ3Index.name

  override val version: Int = XZ3Index.version

  override val serializedWithId: Boolean = XZ3Index.serializedWithId

  override def supports(sft: SimpleFeatureType): Boolean = XZ3Index.supports(sft)
}*/

case object XZ3Index extends AccumuloFeatureIndexType with XZ3WritableIndex with XZ3QueryableIndex {

  def numSplits(sft: SimpleFeatureType): Int = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getZShards
  }
  def splitArrays(sft: SimpleFeatureType): Seq[Array[Byte]] = SplitArrays.getSplitArray(numSplits(sft))

  override val name: String = "xz3"

  override val version: Int = 1

  override val serializedWithId: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.nonPoints && sft.getDtgField.isDefined
  }
}
