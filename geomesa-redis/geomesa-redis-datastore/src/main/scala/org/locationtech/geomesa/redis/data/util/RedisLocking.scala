/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data.util

import java.util.UUID

import org.locationtech.geomesa.index.DistributedLockTimeout
import org.locationtech.geomesa.index.utils.{DistributedLocking, Releasable}
import org.locationtech.geomesa.utils.io.WithClose
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.SetParams

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

  def connection: JedisPool

  override protected def acquireDistributedLock(key: String): Releasable =
    acquireDistributedLock(key, Long.MaxValue).orNull

  override protected def acquireDistributedLock(key: String, timeOut: Long): Option[Releasable] = {
    val start = System.currentTimeMillis()
    var lock: Releasable = null

    while (lock == null && System.currentTimeMillis() - start < timeOut) {
      if (WithClose(connection.getResource)(_.set(key, id, params)) != null) {
        lock = new JedisReleasable(key)
      }
    }

    Option(lock)
  }

  private class JedisReleasable(key: String) extends Releasable {
    override def release(): Unit = {
      WithClose(connection.getResource) { jedis =>
        if (jedis.get(key) == id) {
          jedis.del(key)
        }
      }
    }
  }
}
