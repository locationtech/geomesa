/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z2

import java.nio.charset.StandardCharsets

import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.google.common.primitives.{Bytes, Longs}
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

trait XZ2WritableIndex extends AccumuloWritableIndex {

  import AccumuloWritableIndex.{DefaultSplitArrays, DefaultNumSplits}
  import XZ2Index.SFC

  override def writer(sft: SimpleFeatureType, ops: AccumuloDataStore): (WritableFeature) => Seq[Mutation] = {
    val sharing = sft.getTableSharingBytes
    require(sharing.length < 2, s"Expecting only a single byte for table sharing, got ${sft.getTableSharingPrefix}")
    (wf: WritableFeature) => {
      val mutation = new Mutation(getRowKey(sharing)(wf))
      wf.fullValues.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
      wf.binValues.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
      Seq(mutation)
    }
  }

  override def remover(sft: SimpleFeatureType, ops: AccumuloDataStore): (WritableFeature) => Seq[Mutation] = {
    val sharing = sft.getTableSharingBytes
    (wf: WritableFeature) => {
      val mutation = new Mutation(getRowKey(sharing)(wf))
      wf.fullValues.foreach(value => mutation.putDelete(value.cf, value.cq, value.vis))
      wf.binValues.foreach(value => mutation.putDelete(value.cf, value.cq, value.vis))
      Seq(mutation)
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String = {
    val offset = if (sft.isTableSharing) 10 else 9 // table sharing + shard + 8 byte long
    (row: Text) => new String(row.getBytes, offset, row.getLength - offset, StandardCharsets.UTF_8)
  }

  // table sharing (0-1 byte), split(1 byte), xz value (8 bytes), id (n bytes)
  private def getRowKey(tableSharing: Array[Byte])(wf: WritableFeature): Array[Byte] = {
    val split = DefaultSplitArrays(wf.idHash % DefaultNumSplits)
    val envelope = wf.feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
    val xz = SFC.index(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
    Bytes.concat(tableSharing, split, Longs.toByteArray(xz), id)
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
      DefaultSplitArrays.drop(1).map(s => new Text(ts ++ s)).toSet
    } else {
      DefaultSplitArrays.drop(1).map(new Text(_)).toSet
    }
    val splitsToAdd = splits -- ops.tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      ops.tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }
  }
}
