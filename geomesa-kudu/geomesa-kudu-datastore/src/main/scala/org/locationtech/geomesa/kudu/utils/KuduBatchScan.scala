/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.utils

import java.util.concurrent._

import org.apache.kudu.Schema
import org.apache.kudu.client._
import org.apache.kudu.util.Slice
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.kudu.utils.KuduBatchScan.KuduAbstractBatchScan
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.JavaConverters._

/**
  * Batch scanner for Kudu
  *
  * Note: RowResults are re-used inside the RowResultIterator, so we have to do
  * multi-threading at the RowResultIterator level
  *
  * @param client kudu client
  * @param table table to read
  * @param columns columns to read
  * @param ranges ranges to scan
  * @param predicates predicates
  * @param threads number of threads
  * @param buffer size of output buffer
  */
private class KuduBatchScan(
    client: KuduClient,
    table: KuduTable,
    columns: java.util.List[Integer],
    ranges: Seq[(Option[PartialRow], Option[PartialRow])],
    predicates: Seq[KuduPredicate],
    threads: Int,
    buffer: Int
  ) extends KuduAbstractBatchScan(ranges, threads, buffer, KuduBatchScan.Sentinel) {

  override protected def scan(range: (Option[PartialRow], Option[PartialRow]),
                              out: BlockingQueue[RowResultIterator]): Unit = {
    val builder = client.newScannerBuilder(table).setProjectedColumnIndexes(columns)
    range._1.foreach(builder.lowerBound)
    range._2.foreach(builder.exclusiveUpperBound)
    predicates.foreach(builder.addPredicate)
    val scanner = builder.build()
    try {
      while (scanner.hasMoreRows) {
        out.put(scanner.nextRows())
      }
    } finally {
      scanner.close()
    }
  }
}

object KuduBatchScan {

  type KuduAbstractBatchScan = AbstractBatchScan[(Option[PartialRow], Option[PartialRow]), RowResultIterator]

  private val Sentinel: RowResultIterator = {
    // use reflection to access the private constructor
    // private RowResultIterator(long ellapsedMillis, String tsUUID, Schema schema, int numRows, Slice bs, Slice indirectBs)
    val constructor = classOf[RowResultIterator].getDeclaredConstructor(classOf[Long], classOf[String],
      classOf[Schema], classOf[Int], classOf[Slice], classOf[Slice])
    constructor.setAccessible(true)
    constructor.newInstance(Long.box(0L), null, null, Int.box(0), null, null)
  }

  def apply(
      client: KuduClient,
      table: String,
      columns: Seq[String],
      ranges: Seq[(Option[PartialRow], Option[PartialRow])],
      predicates: Seq[KuduPredicate],
      threads: Int): CloseableIterator[RowResultIterator] = {
    val kuduTable = client.openTable(table)
    val cols = columns.map(kuduTable.getSchema.getColumnIndex).asJava.asInstanceOf[java.util.List[Integer]]
    new KuduBatchScan(client, kuduTable, cols, ranges, predicates, threads, 1000).start()
  }
}
