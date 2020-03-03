/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

import java.util.concurrent._

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.hbase.rpc.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.utils.collection.CloseableIterator

private class CoprocessorBatchScan(
    table: Table,
    ranges: Seq[Scan],
    options: Map[String, String],
    threads: Int,
    buffer: Int
  ) extends AbstractBatchScan[Scan, Array[Byte]](ranges, threads, buffer, CoprocessorBatchScan.Sentinel) {

  override protected def scan(range: Scan, out: BlockingQueue[Array[Byte]]): Unit = {
    val scan = GeoMesaCoprocessor.execute(table, range, options)
    try {
      scan.foreach { r =>
        if (r.size() > 0) {
          out.put(r.toByteArray)
        }
      }
    } finally {
      scan.close()
    }
  }

  override def close(): Unit = {
    super.close()
    table.close()
  }
}

object CoprocessorBatchScan {

  private val Sentinel = Array.empty[Byte]
  private val BufferSize = HBaseSystemProperties.ScanBufferSize.toInt.get

  def apply(
      connection: Connection,
      table: TableName,
      ranges: Seq[Scan],
      options: Map[String, String],
      threads: Int): CloseableIterator[Array[Byte]] = {
    new CoprocessorBatchScan(connection.getTable(table), ranges, options, threads, BufferSize).start()
  }
}
