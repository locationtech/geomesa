/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

import java.util.concurrent._

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.index.utils.AbstractBatchScan

class HBaseBatchScan(connection: Connection, tableName: TableName, ranges: Seq[Scan], threads: Int, buffer: Int)
    extends {
      // use early initialization to ensure table is open before scans kick off
      private val table = connection.getTable(tableName)
    } with AbstractBatchScan[Scan, Result](ranges, threads, buffer) {

  override def close(): Unit = {
    super.close()
    table.close()
  }

  override protected def singletonSentinel: Result = HBaseBatchScan.Sentinel

  override protected def scan(range: Scan, out: BlockingQueue[Result]): Unit = {
    import scala.collection.JavaConversions._

    val scan = table.getScanner(range)
    try {
      scan.iterator.foreach(out.put)
    } finally {
      scan.close()
    }
  }
}

object HBaseBatchScan {
  private val Sentinel = new Result
}
