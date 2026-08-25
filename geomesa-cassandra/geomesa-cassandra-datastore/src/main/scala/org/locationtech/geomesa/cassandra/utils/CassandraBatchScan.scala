/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.utils

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession, ProtocolVersion}
import com.datastax.oss.driver.api.core.cql.{ColumnDefinitions, Row, SimpleStatement}
import com.datastax.oss.driver.api.core.detach.AttachmentPoint
import org.locationtech.geomesa.cassandra.data.CassandraQueryPlan
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.nio.ByteBuffer

private class CassandraBatchScan(session: CqlSession, ranges: Seq[SimpleStatement], threads: Int, buffer: Int)
    extends AbstractBatchScan[SimpleStatement, Row](ranges, threads, buffer, CassandraBatchScan.Sentinel) {

  override protected def scan(range: SimpleStatement): CloseableIterator[Row] =
    CloseableIterator(session.execute(range).iterator())
}

object CassandraBatchScan {

  private val Sentinel: Row = new Row {
    override def getColumnDefinitions: ColumnDefinitions = null
    override def firstIndexOf(id: CqlIdentifier): Int = -1
    override def getType(id: CqlIdentifier): DataType = null
    override def firstIndexOf(name: String): Int = -1
    override def getType(name: String): DataType = null
    override def getBytesUnsafe(i: Int): ByteBuffer = null
    override def size(): Int = 0
    override def getType(i: Int): DataType = null
    override def codecRegistry(): CodecRegistry = null
    override def protocolVersion(): ProtocolVersion = ProtocolVersion.DEFAULT
    override def isDetached: Boolean = true
    override def attach(attachmentPoint: AttachmentPoint): Unit = ()
  }

  def apply(
      plan: CassandraQueryPlan,
      session: CqlSession,
      ranges: Seq[SimpleStatement],
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
