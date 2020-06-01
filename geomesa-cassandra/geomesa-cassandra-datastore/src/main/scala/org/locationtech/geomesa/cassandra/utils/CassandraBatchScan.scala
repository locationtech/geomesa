/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.utils

import java.nio.ByteBuffer

import com.datastax.driver.core._
import org.locationtech.geomesa.cassandra.data.CassandraQueryPlan
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.filter.Filter

private class CassandraBatchScan(session: Session, ranges: Seq[Statement], threads: Int, buffer: Int)
    extends AbstractBatchScan[Statement, Row](ranges, threads, buffer, CassandraBatchScan.Sentinel) {

  override protected def scan(range: Statement): CloseableIterator[Row] =
    CloseableIterator(session.execute(range).iterator())
}

object CassandraBatchScan {

  private val Sentinel: Row = new AbstractGettableData(ProtocolVersion.NEWEST_SUPPORTED) with Row {
    override def getIndexOf(name: String): Int = -1
    override def getColumnDefinitions: ColumnDefinitions = null
    override def getToken(i: Int): Token = null
    override def getToken(name: String): Token = null
    override def getPartitionKeyToken: Token = null
    override def getType(i: Int): DataType = null
    override def getValue(i: Int): ByteBuffer = null
    override def getName(i: Int): String = null
    override def getCodecRegistry: CodecRegistry = null
  }

  def apply(
      plan: CassandraQueryPlan,
      session: Session,
      ranges: Seq[Statement],
      threads: Int,
      timeout: Option[Timeout]): CloseableIterator[Row] = {
    val scanner = new CassandraBatchScan(session, ranges, threads, 100000)
    timeout match {
      case None => scanner.start()
      case Some(t) => new ManagedScanIterator(t, new CassandraScanner(scanner), plan)
    }
  }

  private class ManagedScanIterator(
      override val timeout: Timeout,
      override protected val underlying: CassandraScanner,
      plan: CassandraQueryPlan
    ) extends ManagedScan[Row] {
    override protected def typeName: String = plan.filter.index.sft.getTypeName
    override protected def filter: Option[Filter] = plan.filter.filter
  }

  private class CassandraScanner(scanner: CassandraBatchScan) extends LowLevelScanner[Row] {
    override def iterator: Iterator[Row] = scanner.start()
    override def close(): Unit = scanner.close()
  }
}
