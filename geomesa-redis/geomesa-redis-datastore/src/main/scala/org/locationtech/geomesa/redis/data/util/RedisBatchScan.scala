/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data.util

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

  override protected def scan(range: BoundedByteRange): CloseableIterator[Array[Byte]] = {
    val results = WithClose(connection.getResource)(_.zrangeByLex(table, range.lower, range.upper))
    CloseableIterator(results.iterator())
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
