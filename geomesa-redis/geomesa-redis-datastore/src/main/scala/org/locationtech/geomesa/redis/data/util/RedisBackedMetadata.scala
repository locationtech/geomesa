/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data.util

import java.nio.charset.StandardCharsets

import org.locationtech.geomesa.index.metadata.{CachedLazyBinaryMetadata, MetadataSerializer}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import redis.clients.jedis.JedisPool

/**
  * Redis-backed metadata implementation. Metadata is stored as a redis hashset
  *
  * @param connection connection pool
  * @param table metadata table name
  * @param serializer serializer
  * @tparam T type param
  */
class RedisBackedMetadata[T](connection: JedisPool, table: String, val serializer: MetadataSerializer[T])
    extends CachedLazyBinaryMetadata[T] {

  import scala.collection.JavaConverters._

  val key: Array[Byte] = table.getBytes(StandardCharsets.UTF_8)

  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
    if (rows.lengthCompare(1) == 0) {
      val (k, v) = rows.head
      WithClose(connection.getResource)(_.hset(key, k, v))
    } else {
      val map = new java.util.HashMap[Array[Byte], Array[Byte]](rows.size)
      rows.foreach { case (k, v) => map.put(k, v) }
      WithClose(connection.getResource)(_.hset(key, map))
    }
  }

  override protected def delete(rows: Seq[Array[Byte]]): Unit =
    WithClose(connection.getResource)(_.hdel(key, rows: _*))

  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] =
    Option(WithClose(connection.getResource)(_.hget(key, row)))

  override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[(Array[Byte], Array[Byte])] = {
    val all = WithClose(connection.getResource)(_.hgetAll(key)).asScala.iterator
    prefix match {
      case None    => CloseableIterator(all)
      case Some(p) => CloseableIterator(all.filter { case (k, _) => k.startsWith(p) })
    }
  }

  override protected def checkIfTableExists: Boolean = true

  override protected def createTable(): Unit = {}

  override def close(): Unit = {}
}
