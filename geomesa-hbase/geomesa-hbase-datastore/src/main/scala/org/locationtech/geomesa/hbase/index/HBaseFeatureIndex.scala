/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.KeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoFeatureSerializer, ProjectingKryoFeatureDeserializer}
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase._
import org.locationtech.geomesa.index.api.FilterStrategy
import org.locationtech.geomesa.index.index.IndexAdapter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

object HBaseFeatureIndex extends HBaseIndexManagerType {

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[HBaseFeatureIndex] =
    Seq(HBaseZ3Index, HBaseXZ3Index, HBaseZ2Index, HBaseXZ2Index, HBaseIdIndex)

  override val CurrentIndices: Seq[HBaseFeatureIndex] = AllIndices

  val DataColumnFamily = Bytes.toBytes("d")
  val DataColumnFamilyDescriptor = new HColumnDescriptor(DataColumnFamily)

  val DataColumnQualifier = Bytes.toBytes("d")
  val DataColumnQualifierDescriptor = new HColumnDescriptor(DataColumnQualifier)

  val EmptyBytes = Array.empty[Byte]

  val DefaultNumSplits = 4 // can't be more than Byte.MaxValue (127)
  val DefaultSplitArrays = (0 until DefaultNumSplits).map(_.toByte).toArray.map(Array(_)).toSeq
}

trait HBaseFeatureIndex extends HBaseFeatureIndexType
    with IndexAdapter[HBaseDataStore, HBaseFeature, Mutation, Result, Query] {

  import HBaseFeatureIndex.{DataColumnFamily, DataColumnQualifier}

  /**
    * Retrieve an ID from a row. All indices are assumed to encode the feature ID into the row key
    *
    * @param sft simple feature type
    * @return a function to retrieve an ID from a row
    */
  def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String

  /**
    * Turns accumulo results into simple features
    *
    * @param sft simple feature type
    * @param returnSft return simple feature type (transform, etc)
    * @return
    */
  override protected def entriesToFeatures(sft: SimpleFeatureType, returnSft: SimpleFeatureType): (Result) => SimpleFeature = {
    // Perform a projecting decode of the simple feature
    val getId = getIdFromRow(sft)
    val deserializer = if (sft == returnSft) {
      new KryoFeatureSerializer(sft, SerializationOptions.withoutId)
    } else {
      new ProjectingKryoFeatureDeserializer(sft, returnSft, SerializationOptions.withoutId)
    }
    (result) => {
      val entries = result.getFamilyMap(DataColumnFamily)
      val sf = deserializer.deserialize(entries.values.iterator.next)
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(result.getRow))
      sf
    }
  }

  override def configure(sft: SimpleFeatureType, ds: HBaseDataStore): Unit = {
    super.configure(sft, ds)
    val name = TableName.valueOf(getTableName(sft.getTypeName, ds))
    val admin = ds.connection.getAdmin
    try {
      if (!admin.tableExists(name)) {
        val descriptor = new HTableDescriptor(name)
        descriptor.addFamily(HBaseFeatureIndex.DataColumnFamilyDescriptor)
        admin.createTable(descriptor)
      }
    } finally {
      admin.close()
    }
  }

  override def delete(sft: SimpleFeatureType, ds: HBaseDataStore, shared: Boolean): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    if (shared) {
      val table = ds.connection.getTable(TableName.valueOf(getTableName(sft.getTypeName, ds)))
      try {
        val scan = table.getScanner(new Scan().setRowPrefixFilter(sft.getTableSharingBytes).setFilter(new KeyOnlyFilter))
        try {
          scan.iterator.grouped(10000).foreach { result =>
            val deletes = result.map(r => new Delete(r.getRow))
            table.delete(deletes)
          }
        } finally {
          scan.close()
        }
      } finally {
        table.close()
      }
    } else {
      val table = TableName.valueOf(getTableName(sft.getTypeName, ds))
      val admin = ds.connection.getAdmin
      try {
        admin.deleteTable(table)
      } finally {
        admin.close()
      }
    }
  }

  override protected def createInsert(row: Array[Byte], feature: HBaseFeature): Mutation =
    new Put(row).addImmutable(feature.fullValue.cf, feature.fullValue.cq, feature.fullValue.value)

  override protected def createDelete(row: Array[Byte], feature: HBaseFeature): Mutation =
    new Delete(row).addFamily(feature.fullValue.cf)

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: HBaseDataStore,
                                  filter: FilterStrategy[HBaseDataStore, HBaseFeature, Mutation, Result],
                                  hints: Hints,
                                  ranges: Seq[Query],
                                  ecql: Option[Filter]): HBaseQueryPlanType = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    if (ranges.isEmpty) { EmptyPlan(filter) } else {
      val table = TableName.valueOf(getTableName(sft.getTypeName, ds))
      val eToF = entriesToFeatures(sft, hints.getReturnSft)
      if (ranges.head.isInstanceOf[Get]) {
        GetPlan(filter, table, ranges.asInstanceOf[Seq[Get]], eToF, filter.filter)
      } else {
        ScanPlan(filter, table, ranges.asInstanceOf[Seq[Scan]], eToF, filter.filter)
      }
    }
  }

  // TODO following prefix?
  override protected def range(start: Array[Byte], end: Array[Byte]): Query =
    new Scan(start, end).addColumn(DataColumnFamily, DataColumnQualifier)

  override protected def rangeExact(row: Array[Byte]): Query =
    new Get(row).addColumn(DataColumnFamily, DataColumnQualifier)

  override protected def rangePrefix(prefix: Array[Byte]): Query =
    new Scan().setRowPrefixFilter(prefix).addColumn(DataColumnFamily, DataColumnQualifier)
}
