/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import java.nio.charset.StandardCharsets

import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.google.common.primitives.{Bytes, Longs}
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.FeatureToMutations
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

object XZ2Table extends GeoMesaTable {

  val SFC = new XZ2SFC(12)

  val NUM_SPLITS = 4 // can't be more than Byte.MaxValue (127)
  val SPLIT_ARRAYS = (0 until NUM_SPLITS).map(_.toByte).toArray.map(Array(_)).toSeq

  override def supports(sft: SimpleFeatureType): Boolean = sft.getSchemaVersion > 9 && sft.nonPoints

  override val suffix: String = "xz2"

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val sharing = sft.getTableSharingBytes
    require(sharing.length < 2, s"Expecting only a single byte for table sharing, got ${sft.getTableSharingPrefix}")
    (wf: WritableFeature) => {
      val mutation = new Mutation(getRowKey(sharing)(wf))
      wf.fullValues.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
      wf.binValues.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
      Seq(mutation)
    }
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
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

  // table sharing (0-1 byte), split(1 byte), z value (8 bytes), id (n bytes)
  private def getRowKey(tableSharing: Array[Byte])(wf: WritableFeature): Array[Byte] = {
    val split = SPLIT_ARRAYS(wf.idHash % NUM_SPLITS)
    val envelope = wf.feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
    val xz = SFC.index(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    val id = wf.feature.getID.getBytes(StandardCharsets.UTF_8)
    Bytes.concat(tableSharing, split, Longs.toByteArray(xz), id)
  }

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    import scala.collection.JavaConversions._

    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    val cfs = Seq(GeoMesaTable.FullColumnFamily, GeoMesaTable.BinColumnFamily)
    val localityGroups = cfs.map(cf => (cf.toString, ImmutableSet.of(cf))).toMap
    tableOps.setLocalityGroups(table, localityGroups)

    // drop first split, otherwise we get an empty tablet
    val splits = if (sft.isTableSharing) {
      val ts = sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
      SPLIT_ARRAYS.drop(1).map(s => new Text(ts ++ s)).toSet
    } else {
      SPLIT_ARRAYS.drop(1).map(new Text(_)).toSet
    }
    val splitsToAdd = splits -- tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }
  }
}
