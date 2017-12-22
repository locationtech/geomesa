/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.filter.KeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes
import org.locationtech.geomesa.hbase._
import org.locationtech.geomesa.hbase.coprocessor.AllCoprocessors
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.index.legacy._
import org.locationtech.geomesa.index.index.ClientSideFiltering
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType

object HBaseFeatureIndex extends HBaseIndexManagerType {

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[HBaseFeatureIndex] =
    Seq(HBaseZ3Index, HBaseZ3IndexV1, HBaseXZ3Index, HBaseZ2Index, HBaseZ2IndexV1, HBaseXZ2Index,
      HBaseIdIndex, HBaseAttributeIndex, HBaseAttributeIndexV3, HBaseAttributeIndexV2, HBaseAttributeIndexV1)

  override val CurrentIndices: Seq[HBaseFeatureIndex] =
    Seq(HBaseZ3Index, HBaseXZ3Index, HBaseZ2Index, HBaseXZ2Index, HBaseIdIndex, HBaseAttributeIndex)

  override def indices(sft: SimpleFeatureType, mode: IndexMode): Seq[HBaseFeatureIndex] =
    super.indices(sft, mode).asInstanceOf[Seq[HBaseFeatureIndex]]
  override def index(identifier: String): HBaseFeatureIndex =
    super.index(identifier).asInstanceOf[HBaseFeatureIndex]

  val DataColumnFamily: Array[Byte] = Bytes.toBytes("d")
  val DataColumnFamilyDescriptor = new HColumnDescriptor(DataColumnFamily)

  val DataColumnQualifier: Array[Byte] = Bytes.toBytes("d")
  val DataColumnQualifierDescriptor = new HColumnDescriptor(DataColumnQualifier)
}

trait HBaseFeatureIndex extends HBaseFeatureIndexType with ClientSideFiltering[Result] with LazyLogging {

  override def configure(sft: SimpleFeatureType, ds: HBaseDataStore): Unit = {
    super.configure(sft, ds)

    val name = TableName.valueOf(getTableName(sft.getTypeName, ds))
    val admin = ds.connection.getAdmin
    val coprocessorUrl = ds.config.coprocessorUrl.orElse {
      GeoMesaSystemProperties.SystemProperty("geomesa.hbase.coprocessor.path", null).option.map(new Path(_))
    }

    def addCoprocessor(clazz: Class[_ <: Coprocessor], desc: HTableDescriptor): Unit = {
      val name = clazz.getCanonicalName
      if (!desc.getCoprocessors.contains(name)) {
        // TODO: Warn if the path given is different from paths registered in other coprocessors
        // if so, other tables would need updating
        coprocessorUrl match {
          case Some(path) => desc.addCoprocessor(name, path, Coprocessor.PRIORITY_USER, null)
          case None       => desc.addCoprocessor(name)
        }
      }
    }

    try {
      if (!admin.tableExists(name)) {
        logger.debug(s"Creating table $name")
        val descriptor = new HTableDescriptor(name)
        descriptor.addFamily(HBaseFeatureIndex.DataColumnFamilyDescriptor)
        if (ds.config.remoteFilter) {
          import CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY
          // if the coprocessors are installed site-wide don't register them in the table descriptor
          val installed = Option(admin.getConfiguration.get(USER_REGION_COPROCESSOR_CONF_KEY))
          val names = installed.map(_.split(":").toSet).getOrElse(Set.empty[String])
          AllCoprocessors.foreach(c => if (!names.contains(c.getCanonicalName)) { addCoprocessor(c, descriptor) })
        }
        admin.createTable(descriptor, getSplits(sft).toArray)
      }
    } finally {
      admin.close()
    }
  }

  override def removeAll(sft: SimpleFeatureType, ds: HBaseDataStore): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    import scala.collection.JavaConversions._

    val tableName = TableName.valueOf(getTableName(sft.getTypeName, ds))

    WithClose(ds.connection.getTable(tableName)) { table =>
      val scan = new Scan().setFilter(new KeyOnlyFilter)
      if (sft.isTableSharing) {
        scan.setRowPrefixFilter(sft.getTableSharingBytes)
      }
      ds.applySecurity(scan)
      val mutateParams = new BufferedMutatorParams(tableName)
      WithClose(table.getScanner(scan), ds.connection.getBufferedMutator(mutateParams)) { case (scanner, mutator) =>
        scanner.iterator.grouped(10000).foreach { result =>
          // TODO set delete visibilities
          val deletes = result.map(r => new Delete(r.getRow))
          mutator.mutate(deletes)
        }
      }
    }
  }

  override def delete(sft: SimpleFeatureType, ds: HBaseDataStore, shared: Boolean): Unit = {
    if (shared) { removeAll(sft, ds) } else {
      val table = TableName.valueOf(getTableName(sft.getTypeName, ds))
      val admin = ds.connection.getAdmin
      try {
        admin.disableTable(table)
        admin.deleteTable(table)
      } finally {
        admin.close()
      }
    }
  }
}
