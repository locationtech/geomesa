/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z2

import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex._
import org.opengis.feature.simple.SimpleFeatureType

// current version - deprecated non-point support in favor of xz, ids in row key, per-attribute vis
case object Z2Index extends AccumuloFeatureIndexType with Z2WritableIndex with Z2QueryableIndex {

  val Z2IterPriority = 23

  val NUM_SPLITS = 4 // can't be more than Byte.MaxValue (127)
  val SPLIT_ARRAYS = (0 until NUM_SPLITS).map(_.toByte).toArray.map(Array(_)).toSeq

  // the bytes of z we keep for complex geoms
  // 3 bytes is 22 bits of geometry (not including the first 2 bits which aren't used)
  // roughly equivalent to 4 digits of geohash (32^4 == 2^20) and ~20km resolution
  val GEOM_Z_NUM_BYTES = 3
  // mask for zeroing the last (8 - GEOM_Z_NUM_BYTES) bytes
  val GEOM_Z_MASK: Long = Long.MaxValue << (64 - 8 * GEOM_Z_NUM_BYTES)
  // step needed (due to the mask) to bump up the z value for a complex geom
  val GEOM_Z_STEP: Long = 1L << (64 - 8 * GEOM_Z_NUM_BYTES)

  override val name: String = "z2"

  override val version: Int = 2

  override val serializedWithId: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.isPoints
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val sharing = sharingPrefix(sft)
    (wf: AccumuloFeature) => {
      val rows = getPointRowKey(sharing)(wf)
      rows.map { row =>
        val mutation = new Mutation(row)
        wf.fullValues.foreach { value => mutation.put(value.cf, value.cq, value.vis, value.value) }
        wf.binValues.foreach { value => mutation.put(value.cf, value.cq, value.vis, value.value) }
        mutation
      }
    }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val sharing = sharingPrefix(sft)
    (wf: AccumuloFeature) => {
      val rows = getPointRowKey(sharing)(wf)
      rows.map { row =>
        val mutation = new Mutation(row)
        wf.fullValues.foreach { value => mutation.putDelete(value.cf, value.cq, value.vis) }
        wf.binValues.foreach { value => mutation.putDelete(value.cf, value.cq, value.vis) }
        mutation
      }
    }
  }
}

// initial implementation - supports points and non-points
case object Z2IndexV1 extends AccumuloFeatureIndexType with Z2WritableIndex with Z2QueryableIndex {

  override val name: String = "z2"

  override val version: Int = 1

  override val serializedWithId: Boolean = true

  override def supports(sft: SimpleFeatureType): Boolean = sft.getGeometryDescriptor != null

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val sharing = sharingPrefix(sft)
    val getRowKeys: (AccumuloFeature) => Seq[Array[Byte]] =
      if (sft.isPoints) getPointRowKey(sharing) else getGeomRowKeys(sharing)

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
    val getRowKeys: (AccumuloFeature) => Seq[Array[Byte]] =
      if (sft.isPoints) getPointRowKey(sharing) else getGeomRowKeys(sharing)

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