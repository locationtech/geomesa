/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z3

import java.nio.charset.StandardCharsets
import java.util.Date

import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.locationtech.geomesa.curve.BinnedTime.TimeToBinnedTime
import org.locationtech.geomesa.curve.{BinnedTime, XZ3SFC}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

trait XZ3WritableIndex extends AccumuloWritableIndex {

  override def writer(sft: SimpleFeatureType, ops: AccumuloDataStore): (WritableFeature) => Seq[Mutation] = {
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new RuntimeException("Z3 writer requires a valid date"))
    val sfc = XZ3SFC(XZ3Index.Precision, sft.getZ3Interval)
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sharing = sft.getTableSharingBytes
    require(sharing.length < 2, s"Expecting only a single byte for table sharing, got ${sft.getTableSharingPrefix}")
    val rowKey = getRowKey(sfc, timeToIndex, sharing, dtgIndex)_
    (wf: WritableFeature) => {
      val mutation = new Mutation(rowKey(wf))
      wf.fullValues.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
      wf.binValues.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
      Seq(mutation)
    }
  }

  override def remover(sft: SimpleFeatureType, ops: AccumuloDataStore): (WritableFeature) => Seq[Mutation] = {
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new RuntimeException("Z3 writer requires a valid date"))
    val sfc = XZ3SFC(XZ3Index.Precision, sft.getZ3Interval)
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val sharing = sft.getTableSharingBytes
    val rowKey = getRowKey(sfc, timeToIndex, sharing, dtgIndex)_
    (wf: WritableFeature) => {
      val mutation = new Mutation(rowKey(wf))
      wf.fullValues.foreach(value => mutation.putDelete(value.cf, value.cq, value.vis))
      wf.binValues.foreach(value => mutation.putDelete(value.cf, value.cq, value.vis))
      Seq(mutation)
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String = {
    val offset = if (sft.isTableSharing) 12 else 11 // table sharing + shard + 2 byte short + 8 byte long
    (row: Text) => new String(row.getBytes, offset, row.getLength - offset, StandardCharsets.UTF_8)
  }

  // table sharing (0-1 byte), split (1 byte), time interval(2 bytes), z value (8 bytes), id (n bytes)
  private def getRowKey(sfc: XZ3SFC, timeToIndex: TimeToBinnedTime, tableSharing: Array[Byte], dtgIndex: Int)
                       (wf: WritableFeature): Array[Byte] = {
    val split = AccumuloWritableIndex.DefaultSplitArrays(wf.idHash % AccumuloWritableIndex.DefaultNumSplits)
    val envelope = wf.feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
    // TODO support date intervals
    val dtg = wf.feature.getAttribute(dtgIndex).asInstanceOf[Date]
    val time = if (dtg == null) 0 else dtg.getTime
    val BinnedTime(b, t) = timeToIndex(time)
    val xz = sfc.index(envelope.getMinX, envelope.getMinY, t, envelope.getMaxX, envelope.getMaxY, t)
    val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
    Bytes.concat(tableSharing, split, Shorts.toByteArray(b), Longs.toByteArray(xz), id)
  }


  override def configure(sft: SimpleFeatureType, ops: AccumuloDataStore): Unit = {
    import scala.collection.JavaConversions._

    val table = Try(ops.getTableName(sft.getTypeName, this)).getOrElse {
      val table = GeoMesaTable.formatTableName(ops.catalogTable, tableSuffix, sft)
      ops.metadata.insert(sft.getTypeName, tableNameKey, table)
      table
    }
    AccumuloVersion.ensureTableExists(ops.connector, table)
    ops.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    val cfs = Seq(AccumuloWritableIndex.FullColumnFamily, AccumuloWritableIndex.BinColumnFamily)
    val localityGroups = cfs.map(cf => (cf.toString, ImmutableSet.of(cf))).toMap
     ops.tableOps.setLocalityGroups(table, localityGroups)

    // drop first split, otherwise we get an empty tablet
    val splits = if (sft.isTableSharing) {
      val ts = sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
      AccumuloWritableIndex.DefaultSplitArrays.drop(1).map(s => new Text(ts ++ s)).toSet
    } else {
      AccumuloWritableIndex.DefaultSplitArrays.drop(1).map(new Text(_)).toSet
    }
    val splitsToAdd = splits -- ops.tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      ops.tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }
  }
}
