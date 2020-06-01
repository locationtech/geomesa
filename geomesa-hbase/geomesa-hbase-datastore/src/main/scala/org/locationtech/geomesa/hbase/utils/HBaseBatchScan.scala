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
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.opengis.filter.Filter

private class HBaseBatchScan(connection: Connection, table: TableName, ranges: Seq[Scan], threads: Int, buffer: Int)
    extends AbstractBatchScan[Scan, Result](ranges, threads, buffer, HBaseBatchScan.Sentinel) {

  private val htable = connection.getTable(table, new CachedThreadPool(threads))

  override protected def scan(range: Scan): CloseableIterator[Result] = new RangeScanner(range)

  override def close(): Unit = {
    try { super.close() } finally {
      htable.close()
    }
  }

  private class RangeScanner(range: Scan) extends CloseableIterator[Result] {

    private val scan = htable.getScanner(range)
    private var result: Result = _

    override def hasNext: Boolean = {
      if (result != null) {
        true
      } else if (closed) {
        false
      } else {
        result = scan.next()
        result != null
      }
    }

    override def next(): Result = {
      val r = result
      result = null
      r
    }

    override def close(): Unit = scan.close()
  }
}

object HBaseBatchScan {

  private val Sentinel = new Result
  private val BufferSize = HBaseSystemProperties.ScanBufferSize.toInt.get

  /**
   * Creates a batch scan with parallelism across the given scans
   *
   * @param connection connection
   * @param table table to scan
   * @param ranges ranges
   * @param threads number of concurrently running scans
   * @return
   */
  def apply(
      plan: HBaseQueryPlan,
      connection: Connection,
      table: TableName,
      ranges: Seq[Scan],
      threads: Int,
      timeout: Option[Timeout]): CloseableIterator[Result] = {
    val scanner = new HBaseBatchScan(connection, table, ranges, threads, BufferSize)
    timeout match {
      case None => scanner.start()
      case Some(t) => new ManagedScanIterator(t, new HBaseScanner(scanner), plan)
    }
  }

  private class ManagedScanIterator(
      override val timeout: Timeout,
      override protected val underlying: HBaseScanner,
      plan: HBaseQueryPlan
    ) extends ManagedScan[Result] {
    override protected def typeName: String = plan.filter.index.sft.getTypeName
    override protected def filter: Option[Filter] = plan.filter.filter
  }

  private class HBaseScanner(scanner: HBaseBatchScan) extends LowLevelScanner[Result] {
    override def iterator: Iterator[Result] = scanner.start()
    override def close(): Unit = scanner.close()
  }
}
