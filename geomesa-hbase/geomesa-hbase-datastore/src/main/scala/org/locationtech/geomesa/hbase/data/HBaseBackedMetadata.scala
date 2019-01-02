/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.locationtech.geomesa.hbase.utils.HBaseVersions
import org.locationtech.geomesa.index.metadata.{CachedLazyBinaryMetadata, MetadataSerializer}
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.JavaConversions._

class HBaseBackedMetadata[T](val connection: Connection, val catalog: TableName, val serializer: MetadataSerializer[T])
    extends CachedLazyBinaryMetadata[T] {

  import HBaseMetadataAdapter._

  lazy private val table = connection.getTable(catalog)

  override protected def checkIfTableExists: Boolean = {
    val admin = connection.getAdmin
    try { admin.tableExists(catalog) } finally { admin.close() }
  }

  override protected def createTable(): Unit = {
    val admin = connection.getAdmin
    try {
      if (!admin.tableExists(catalog)) {
        val descriptor = new HTableDescriptor(catalog)
        HBaseVersions.addFamily(descriptor, ColumnFamilyDescriptor)
        admin.createTable(descriptor)
      }
    } finally {
      admin.close()
    }
  }

  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit =
    table.put(rows.map { case (r, v) => new Put(r).addColumn(ColumnFamily, ColumnQualifier, v) }.toList)

  override protected def delete(rows: Seq[Array[Byte]]): Unit =
    // note: list passed in must be mutable
    table.delete(rows.map(r => new Delete(r)).toBuffer)

  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = {
    val result = table.get(new Get(row).addColumn(ColumnFamily, ColumnQualifier))
    if (result.isEmpty) { None } else { Option(result.getValue(ColumnFamily, ColumnQualifier)) }
  }

  override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[(Array[Byte], Array[Byte])] = {
    val scan = new Scan().addColumn(ColumnFamily, ColumnQualifier)
    prefix.foreach(scan.setRowPrefixFilter)
    val scanner = table.getScanner(scan)
    val results = scanner.iterator.map(s => (s.getRow, s.getValue(ColumnFamily, ColumnQualifier)))
    CloseableIterator(results, scanner.close())
  }

  override def close(): Unit = table.close()
}

object HBaseMetadataAdapter {
  val ColumnFamily: Array[Byte] = Bytes.toBytes("m")
  val ColumnFamilyDescriptor = new HColumnDescriptor(ColumnFamily)
  val ColumnQualifier: Array[Byte] = Bytes.toBytes("v")
}
