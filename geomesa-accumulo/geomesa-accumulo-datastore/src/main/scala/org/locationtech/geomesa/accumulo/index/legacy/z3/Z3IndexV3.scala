/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.z3

import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.index.utils.SplitArrays
import org.opengis.feature.simple.SimpleFeatureType

// deprecated polygon support in favor of xz, ids in row key, per-attribute vis
case object Z3IndexV3 extends AccumuloFeatureIndex with Z3WritableIndex with Z3QueryableIndex {

  val Z3IterPriority = 23

  // in 1.2.5/6, we stored the CQ as the number of rows, which was always one
  // this wasn't captured correctly in a versioned index class, so we need to delete both possible CQs
  private val DeleteText = new Text("1,")

  override val name: String = "z3"

  override val version: Int = 3

  override val hasSplits: Boolean = true

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = true

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getDtgField.isDefined && sft.isPoints
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    SplitArrays.apply(sft.getZShards)
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)
    val splitArray = SplitArrays.apply(sft.getZShards)

    (wf: AccumuloFeature) => {
      val rows = getPointRowKey(timeToIndex, sfc, splitArray)(wf, dtgIndex)
      rows.map { row =>
        val mutation = new Mutation(row)
        wf.fullValues.foreach { value => mutation.put(value.cf, value.cq, value.vis, value.value) }
        wf.binValues.foreach { value => mutation.put(value.cf, value.cq, value.vis, value.value) }
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

    (wf: AccumuloFeature) => {
      val rows = getPointRowKey(timeToIndex, sfc, splitArray)(wf, dtgIndex)
      rows.map { row =>
        val mutation = new Mutation(row)
        wf.fullValues.foreach { value =>
          mutation.putDelete(value.cf, value.cq, value.vis)
          mutation.putDelete(value.cf, DeleteText, value.vis)
        }
        wf.binValues.foreach { value => mutation.putDelete(value.cf, value.cq, value.vis) }
        mutation
      }
    }
  }
}
