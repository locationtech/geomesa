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
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan
import org.locationtech.geomesa.hbase.rpc.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.opengis.filter.Filter

private class CoprocessorBatchScan(
    connection: Connection,
    table: TableName,
    ranges: Seq[Scan],
    options: Map[String, String],
    rpcThreads: Int,
    buffer: Int
  ) extends AbstractBatchScan[Scan, Array[Byte]](ranges, rpcThreads * 2, buffer, CoprocessorBatchScan.Sentinel) {

  protected val pool = new CachedThreadPool(rpcThreads)

  override protected def scan(range: Scan, out: BlockingQueue[Array[Byte]]): Unit = {
    val results = GeoMesaCoprocessor.execute(connection, table, range, options, pool)
    try {
      results.foreach { r =>
        if (r.size() > 0) {
          out.put(r.toByteArray)
        }
      }
    } finally {
      results.close()
    }
  }

  override def close(): Unit = {
    try { super.close() } finally {
      pool.shutdownNow()
    }
  }
}

object CoprocessorBatchScan {

  private val Sentinel = Array.empty[Byte]
  private val BufferSize = HBaseSystemProperties.ScanBufferSize.toInt.get

  /**
   * Start a coprocessor batch scan
   *
   * @param connection connection
   * @param table table
   * @param ranges ranges to scan
   * @param options coprocessor configuration
   * @param rpcThreads size of thread pool used for hbase rpc calls, across all client scan threads
   * @return
   */
  def apply(
      plan: HBaseQueryPlan,
      connection: Connection,
      table: TableName,
      ranges: Seq[Scan],
      options: Map[String, String],
      rpcThreads: Int,
      timeout: Option[Timeout]): CloseableIterator[Array[Byte]] = {
    val opts = options ++ timeout.map(GeoMesaCoprocessor.timeout)
    val scanner = new CoprocessorBatchScan(connection, table, ranges, opts, rpcThreads, BufferSize)
    timeout match {
      case None => scanner.start()
      case Some(t) => new ManagedCoprocessorIterator(t, new CoprocessorScanner(scanner), plan)
    }
  }

  private class ManagedCoprocessorIterator(
      override val timeout: Timeout,
      override protected val underlying: CoprocessorScanner,
      plan: HBaseQueryPlan
    ) extends ManagedScan[Array[Byte]] {
    override protected def typeName: String = plan.filter.index.sft.getTypeName
    override protected def filter: Option[Filter] = plan.filter.filter
  }

  private class CoprocessorScanner(scanner: CoprocessorBatchScan) extends LowLevelScanner[Array[Byte]] {
    override def iterator: Iterator[Array[Byte]] = scanner.start()
    override def close(): Unit = scanner.close()
  }
}
