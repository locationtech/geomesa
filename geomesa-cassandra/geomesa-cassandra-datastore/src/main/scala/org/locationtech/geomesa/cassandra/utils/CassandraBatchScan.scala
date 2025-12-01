/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.utils

import com.datastax.driver.core._
import org.locationtech.geomesa.cassandra.data.CassandraQueryPlan
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.nio.ByteBuffer

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
      case Some(t) => new ManagedScan(new CassandraScanner(scanner), t, plan)
    }
  }

  private class CassandraScanner(scanner: CassandraBatchScan) extends LowLevelScanner[Row] {
    override def iterator: Iterator[Row] = scanner.start()
    override def close(): Unit = scanner.close()
  }
}
