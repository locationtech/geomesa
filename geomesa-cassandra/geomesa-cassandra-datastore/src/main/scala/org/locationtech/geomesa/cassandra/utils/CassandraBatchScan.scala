/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.utils

import java.nio.ByteBuffer
import java.util.concurrent._

import com.datastax.driver.core._
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.utils.collection.CloseableIterator

private class CassandraBatchScan(session: Session, ranges: Seq[Statement], threads: Int, buffer: Int)
    extends AbstractBatchScan[Statement, Row](ranges, threads, buffer, CassandraBatchScan.Sentinel) {

  override protected def scan(range: Statement, out: BlockingQueue[Row]): Unit = {
    val results = session.execute(range).iterator()
    while (results.hasNext) {
      out.put(results.next)
    }
  }
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

  def apply(session: Session, ranges: Seq[Statement], threads: Int): CloseableIterator[Row] =
    new CassandraBatchScan(session, ranges, threads, 100000).start()
}
