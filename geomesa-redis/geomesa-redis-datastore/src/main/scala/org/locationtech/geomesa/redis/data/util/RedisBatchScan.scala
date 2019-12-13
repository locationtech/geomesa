/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data.util

import java.util.concurrent.BlockingQueue

import org.locationtech.geomesa.index.api.BoundedByteRange
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import redis.clients.jedis.JedisPool

private class RedisBatchScan(
    connection: JedisPool,
    table: Array[Byte],
    ranges: Seq[BoundedByteRange],
    threads: Int,
    buffer: Int
  ) extends AbstractBatchScan[BoundedByteRange, Array[Byte]](ranges, threads, buffer, RedisBatchScan.Sentinel) {

  override protected def scan(range: BoundedByteRange, out: BlockingQueue[Array[Byte]]): Unit = {
    val iter = WithClose(connection.getResource)(_.zrangeByLex(table, range.lower, range.upper)).iterator()
    while (iter.hasNext) {
      out.put(iter.next())
    }
  }
}

object RedisBatchScan {

  private val Sentinel = new Array[Byte](0)

  def apply(
      connection: JedisPool,
      table: Array[Byte],
      ranges: Seq[BoundedByteRange],
      threads: Int): CloseableIterator[Array[Byte]] =
    new RedisBatchScan(connection, table, ranges, threads, 100000).start()
}
