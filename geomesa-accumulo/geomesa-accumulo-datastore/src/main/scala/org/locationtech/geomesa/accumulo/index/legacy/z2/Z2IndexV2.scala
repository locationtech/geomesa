/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.z2

import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.index.utils.SplitArrays
import org.opengis.feature.simple.SimpleFeatureType

// deprecated non-point support in favor of xz, ids in row key, per-attribute vis
case object Z2IndexV2 extends AccumuloFeatureIndex with Z2WritableIndex with Z2QueryableIndex {

  val Z2IterPriority = 23

  // in 1.2.5/6, we stored the CQ as the number of rows, which was always one
  // this wasn't captured correctly in a versioned index class, so we need to delete both possible CQs
  private val DeleteText = new Text("1,")

  override val name: String = "z2"

  override val version: Int = 2

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = true

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.isPoints
  }

  override def getSplits(sft: SimpleFeatureType, partition: Option[String]): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    SplitArrays.apply(sft.getZShards)
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val sharing = sharingPrefix(sft)
    val splitArray = SplitArrays.apply(sft.getZShards)

    (wf: AccumuloFeature) => {
      val rows = getPointRowKey(sharing, splitArray, lenient = false)(wf)
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
    val sharing = sharingPrefix(sft)
    val splitArray = SplitArrays.apply(sft.getZShards)
    (wf: AccumuloFeature) => {
      val rows = getPointRowKey(sharing, splitArray, lenient = true)(wf)
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
