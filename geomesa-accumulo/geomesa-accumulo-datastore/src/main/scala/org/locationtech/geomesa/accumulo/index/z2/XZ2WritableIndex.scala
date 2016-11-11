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
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

trait XZ2WritableIndex extends AccumuloWritableIndex {

  import AccumuloWritableIndex.{DefaultNumSplits, DefaultSplitArrays}

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val sfc = XZ2SFC(sft.getXZPrecision)
    val sharing = sft.getTableSharingBytes
    require(sharing.length < 2, s"Expecting only a single byte for table sharing, got ${sft.getTableSharingPrefix}")
    (wf: AccumuloFeature) => {
      val mutation = new Mutation(getRowKey(sfc, sharing)(wf))
      wf.fullValues.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
      wf.binValues.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
      Seq(mutation)
    }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val sfc = XZ2SFC(sft.getXZPrecision)
    val sharing = sft.getTableSharingBytes
    (wf: AccumuloFeature) => {
      val mutation = new Mutation(getRowKey(sfc, sharing)(wf))
      wf.fullValues.foreach(value => mutation.putDelete(value.cf, value.cq, value.vis))
      wf.binValues.foreach(value => mutation.putDelete(value.cf, value.cq, value.vis))
      Seq(mutation)
    }
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    val start = if (sft.isTableSharing) { 10 } else { 9 } // table sharing + shard + 8 byte long
    (row, offset, length) => new String(row, offset + start, length - start, StandardCharsets.UTF_8)
  }

  // table sharing (0-1 byte), split(1 byte), xz value (8 bytes), id (n bytes)
  private def getRowKey(sfc: XZ2SFC, tableSharing: Array[Byte])(wf: AccumuloFeature): Array[Byte] = {
    val split = DefaultSplitArrays(wf.idHash % DefaultNumSplits)
    val envelope = wf.feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
    val xz = sfc.index(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
    Bytes.concat(tableSharing, split, Longs.toByteArray(xz), id)
  }

  override def configure(sft: SimpleFeatureType, ds: AccumuloDataStore): Unit = {
    import scala.collection.JavaConversions._

    super.configure(sft, ds)
    val table = getTableName(sft.getTypeName, ds)

    AccumuloVersion.ensureTableExists(ds.connector, table)

    val cfs = Seq(AccumuloWritableIndex.FullColumnFamily, AccumuloWritableIndex.BinColumnFamily)
    val localityGroups = cfs.map(cf => (cf.toString, ImmutableSet.of(cf))).toMap
    ds.tableOps.setLocalityGroups(table, localityGroups)

    // drop first split, otherwise we get an empty tablet
    val splits = if (sft.isTableSharing) {
      val ts = sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
      DefaultSplitArrays.drop(1).map(s => new Text(ts ++ s)).toSet
    } else {
      DefaultSplitArrays.drop(1).map(new Text(_)).toSet
    }
    val splitsToAdd = splits -- ds.tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      ds.tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }

    ds.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
  }
}
