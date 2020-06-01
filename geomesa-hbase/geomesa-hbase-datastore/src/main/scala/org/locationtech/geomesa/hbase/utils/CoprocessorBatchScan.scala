/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

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
    scanThreads: Int,
    rpcThreads: Int,
    buffer: Int
  ) extends AbstractBatchScan[Scan, Array[Byte]](ranges, scanThreads, buffer, CoprocessorBatchScan.Sentinel) {

  protected val pool = new CachedThreadPool(rpcThreads)

  override protected def scan(range: Scan): CloseableIterator[Array[Byte]] = {
    GeoMesaCoprocessor.execute(connection, table, range, options, pool).collect {
      case r if r.size() > 0 => r.toByteArray
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
      scanThreads: Int,
      rpcThreads: Int,
      timeout: Option[Timeout]): CloseableIterator[Array[Byte]] = {
    val opts = options ++ timeout.map(GeoMesaCoprocessor.timeout)
    val scanner = new CoprocessorBatchScan(connection, table, ranges, opts, scanThreads, rpcThreads, BufferSize)
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
