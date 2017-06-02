/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.z2

import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature, EMPTY_TEXT}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.{BinColumnFamily, FullColumnFamily}
import org.locationtech.geomesa.index.utils.SplitArrays
import org.opengis.feature.simple.SimpleFeatureType

// initial implementation - supports points and non-points
case object Z2IndexV1 extends AccumuloFeatureIndex with Z2WritableIndex with Z2QueryableIndex {

  // the bytes of z we keep for complex geoms
  // 3 bytes is 22 bits of geometry (not including the first 2 bits which aren't used)
  // roughly equivalent to 4 digits of geohash (32^4 == 2^20) and ~20km resolution
  val GEOM_Z_NUM_BYTES = 3
  // mask for zeroing the last (8 - GEOM_Z_NUM_BYTES) bytes
  val GEOM_Z_MASK: Long = Long.MaxValue << (64 - 8 * GEOM_Z_NUM_BYTES)
  // step needed (due to the mask) to bump up the z value for a complex geom
  val GEOM_Z_STEP: Long = 1L << (64 - 8 * GEOM_Z_NUM_BYTES)

  override val name: String = "z2"

  override val version: Int = 1

  override val serializedWithId: Boolean = true

  override val hasPrecomputedBins: Boolean = true

  override def supports(sft: SimpleFeatureType): Boolean = sft.getGeometryDescriptor != null

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    SplitArrays.apply(sft.getZShards)
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val sharing = sharingPrefix(sft)
    val splitArray = SplitArrays.apply(sft.getZShards)
    val getRowKeys: (AccumuloFeature) => Seq[Array[Byte]] =
      if (sft.isPoints) getPointRowKey(sharing, splitArray) else getGeomRowKeys(sharing, splitArray)

    (wf: AccumuloFeature) => {
      val rows = getRowKeys(wf)
      // store the duplication factor in the column qualifier for later use
      val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
      rows.map { row =>
        val mutation = new Mutation(row)
        wf.fullValuesWithId.foreach(value => mutation.put(FullColumnFamily, cq, value.vis, value.value))
        wf.binValues.foreach(value => mutation.put(BinColumnFamily, cq, value.vis, value.value))
        mutation
      }
    }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val sharing = sharingPrefix(sft)
    val splitArray = SplitArrays.apply(sft.getZShards)
    val getRowKeys: (AccumuloFeature) => Seq[Array[Byte]] =
      if (sft.isPoints) getPointRowKey(sharing, splitArray) else getGeomRowKeys(sharing, splitArray)

    (wf: AccumuloFeature) => {
      val rows = getRowKeys(wf)
      val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
      rows.map { row =>
        val mutation = new Mutation(row)
        wf.fullValuesWithId.foreach(value => mutation.putDelete(FullColumnFamily, cq, value.vis))
        wf.binValues.foreach(value => mutation.putDelete(BinColumnFamily, cq, value.vis))
        mutation
      }
    }
  }
}
