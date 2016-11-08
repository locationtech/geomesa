/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z3

import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex._
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.opengis.feature.simple.SimpleFeatureType

// current version - deprecated polygon support in favor of xz, ids in row key, per-attribute vis
case object Z3Index extends AccumuloFeatureIndexType with Z3WritableIndex with Z3QueryableIndex {

  val Z3IterPriority = 23

  // the bytes of z we keep for complex geoms
  // 3 bytes is 15 bits of geometry (not including time bits and the first 2 bits which aren't used)
  // roughly equivalent to 3 digits of geohash (32^3 == 2^15) and ~78km resolution
  // (4 bytes is 20 bits, equivalent to 4 digits of geohash and ~20km resolution)
  // note: we also lose time resolution
  val GEOM_Z_NUM_BYTES = 3
  // mask for zeroing the last (8 - GEOM_Z_NUM_BYTES) bytes
  val GEOM_Z_MASK: Long = Long.MaxValue << (64 - 8 * GEOM_Z_NUM_BYTES)
  // step needed (due to the mask) to bump up the z value for a complex geom
  val GEOM_Z_STEP: Long = 1L << (64 - 8 * GEOM_Z_NUM_BYTES)

  override val name: String = "z3"

  override val version: Int = 3

  override val serializedWithId: Boolean = false

  override val hasSplits: Boolean = true

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getDtgField.isDefined && sft.isPoints
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)

    (wf: AccumuloFeature) => {
      val rows = getPointRowKey(timeToIndex, sfc)(wf, dtgIndex)
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

    (wf: AccumuloFeature) => {
      val rows = getPointRowKey(timeToIndex, sfc)(wf, dtgIndex)
      rows.map { row =>
        val mutation = new Mutation(row)
        wf.fullValues.foreach { value => mutation.putDelete(value.cf, value.cq, value.vis) }
        wf.binValues.foreach { value => mutation.putDelete(value.cf, value.cq, value.vis) }
        mutation
      }
    }
  }
}

// polygon support and splits
case object Z3IndexV2 extends AccumuloFeatureIndexType with Z3WritableIndex with Z3QueryableIndex {

  override val name: String = "z3"

  override val version: Int = 2

  override val serializedWithId: Boolean = true

  override val hasSplits: Boolean = true

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getDtgField.isDefined && sft.getGeometryDescriptor != null
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)
    val getRowKeys: (AccumuloFeature, Int) => Seq[Array[Byte]] =
      if (sft.isPoints) { getPointRowKey(timeToIndex, sfc) } else { getGeomRowKeys(timeToIndex, sfc) }

    (wf: AccumuloFeature) => {
      val rows = getRowKeys(wf, dtgIndex)
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
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)
    val getRowKeys: (AccumuloFeature, Int) => Seq[Array[Byte]] =
      if (sft.isPoints) { getPointRowKey(timeToIndex, sfc) } else { getGeomRowKeys(timeToIndex, sfc) }

    (wf: AccumuloFeature) => {
      val rows = getRowKeys(wf, dtgIndex)
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

// initial z3 implementation - only supports points
case object Z3IndexV1 extends AccumuloFeatureIndexType with Z3WritableIndex with Z3QueryableIndex {

  override val name: String = "z3"

  override val version: Int = 1

  override val serializedWithId: Boolean = true

  override val hasSplits: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getDtgField.isDefined && sft.isPoints
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sfc = Z3SFC(sft.getZ3Interval)
    val getRowKeys: (AccumuloFeature, Int) => Seq[Array[Byte]] =
      (wf, i) => getPointRowKey(timeToIndex, sfc)(wf, i).map(_.drop(1))

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
    val getRowKeys: (AccumuloFeature, Int) => Seq[Array[Byte]] =
      (ftw, i) => getPointRowKey(timeToIndex, sfc)(ftw, i).map(_.drop(1))

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