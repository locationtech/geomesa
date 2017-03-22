/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.z3

import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature, EMPTY_TEXT}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.{BinColumnFamily, FullColumnFamily}
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.index.utils.SplitArrays
import org.opengis.feature.simple.SimpleFeatureType

// initial z3 implementation - only supports points
case object Z3IndexV1 extends AccumuloFeatureIndex with Z3WritableIndex with Z3QueryableIndex {

  override val name: String = "z3"

  override val version: Int = 1

  override val hasSplits: Boolean = false

  override val serializedWithId: Boolean = true

  override val hasPrecomputedBins: Boolean = true

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getDtgField.isDefined && sft.isPoints
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = Seq.empty

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)
    val splitArray = SplitArrays.apply(sft.getZShards)
    val getRowKeys: (AccumuloFeature, Int) => Seq[Array[Byte]] =
      (wf, i) => getPointRowKey(timeToIndex, sfc, splitArray)(wf, i).map(_.drop(1))

    (wf: AccumuloFeature) => {
      val rows = getRowKeys(wf, dtgIndex)
      rows.map { row =>
        val mutation = new Mutation(row)
        wf.fullValuesWithId.foreach(value => mutation.put(FullColumnFamily, EMPTY_TEXT, value.vis, value.value))
        wf.binValues.foreach(value => mutation.put(BinColumnFamily, EMPTY_TEXT, value.vis, value.value))
        mutation
      }
    }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)
    val splitArray = SplitArrays.apply(sft.getZShards)
    val getRowKeys: (AccumuloFeature, Int) => Seq[Array[Byte]] =
      (ftw, i) => getPointRowKey(timeToIndex, sfc, splitArray)(ftw, i).map(_.drop(1))

    (wf: AccumuloFeature) => {
      val rows = getRowKeys(wf, dtgIndex)
      rows.map { row =>
        val mutation = new Mutation(row)
        wf.fullValuesWithId.foreach(value => mutation.putDelete(FullColumnFamily, EMPTY_TEXT, value.vis))
        wf.binValues.foreach(value => mutation.putDelete(BinColumnFamily, EMPTY_TEXT, value.vis))
        mutation
      }
    }
  }
}
