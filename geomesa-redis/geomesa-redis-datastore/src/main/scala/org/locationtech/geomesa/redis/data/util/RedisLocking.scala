/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.redis.data.util

import org.locationtech.geomesa.index.DistributedLockTimeout
import org.locationtech.geomesa.index.utils.DistributedLocking
import org.locationtech.geomesa.redis.data.CloseableJedisCommands
import org.locationtech.geomesa.utils.io.WithClose
import redis.clients.jedis.params.SetParams
import redis.clients.jedis.util.Pool

import java.io.Closeable
import java.util.UUID

/**
  * Implements the basic single-node locking scheme from https://redis.io/topics/distlock
  *
  * The lock is only considered valid for a duration of `geomesa.distributed.lock.timeout` (default 2 minutes).
  *
  * Note: exclusivity when holding the lock is not 100% guaranteed
  */
trait RedisLocking extends DistributedLocking {

  private val id = UUID.randomUUID().toString

  private val params = {
    val timeout = DistributedLockTimeout.toDuration.getOrElse {
      // note: should always be a valid fallback value so this exception should never be triggered
      throw new IllegalArgumentException(s"Couldn't convert '${DistributedLockTimeout.get}' to a duration")
    }
    new SetParams().nx().px(timeout.toMillis)
  }

  def connection: Pool[_ <: CloseableJedisCommands]

  override protected def acquireDistributedLock(key: String): Closeable =
    acquireDistributedLock(key, Long.MaxValue).orNull

  override protected def acquireDistributedLock(key: String, timeOut: Long): Option[Closeable] = {
    val start = System.currentTimeMillis()
    var lock: Closeable = null

    while (lock == null && System.currentTimeMillis() - start < timeOut) {
      if (WithClose(connection.getResource)(_.set(key, id, params)) != null) {
        lock = new JedisReleasable(key)
      }
    }

    Option(lock)
  }

  private class JedisReleasable(key: String) extends Closeable {
    override def close(): Unit = {
      WithClose(connection.getResource) { jedis =>
        if (jedis.get(key) == id) {
          jedis.del(key)
        }
      }
    }
  }
}
