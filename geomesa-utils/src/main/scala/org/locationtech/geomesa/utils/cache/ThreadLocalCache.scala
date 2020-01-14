/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import java.io.Closeable
import java.lang.ref.WeakReference
import java.util.concurrent._

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.google.common.util.concurrent.MoreExecutors

import scala.concurrent.duration.Duration

/**
 * Creates a per-thread cache of values with a timed expiration.
 *
 * Since caches are only cleaned up when accessed, uses an asynchronous thread to
 * actively clean up any orphaned thread local values
 *
 * @tparam K key type
 * @tparam V value type
 */
class ThreadLocalCache[K <: AnyRef, V <: AnyRef](
    expiry: Duration,
    executor: ScheduledExecutorService = ThreadLocalCache.executor
  ) extends scala.collection.mutable.Map[K, V]
    with scala.collection.mutable.MapLike[K, V, ThreadLocalCache[K, V]]
    with Runnable
    with Closeable {

  import scala.collection.JavaConverters._

  // weak references to our current caches, to allow cleanup + GC
  private val references = new ConcurrentLinkedQueue[WeakReference[Cache[K, V]]]()

  private val caches = new ThreadLocal[Cache[K, V]]() {
    override def initialValue(): Cache[K, V] = {
      val cache = Caffeine.newBuilder().expireAfterAccess(expiry.toMillis, TimeUnit.MILLISECONDS).build[K, V]()
      references.offer(new WeakReference(cache)) // this will always succeed as our queue is unbounded
      cache
    }
  }

  private val cleanup = executor.scheduleWithFixedDelay(this, expiry.toMillis, expiry.toMillis, TimeUnit.MILLISECONDS)

  override def get(key: K): Option[V] = Option(caches.get.getIfPresent(key))

  override def getOrElseUpdate(key: K, op: => V): V = {
    val cached = caches.get.getIfPresent(key)
    if (cached != null) { cached } else {
      val value = op
      caches.get.put(key, value)
      value
    }
  }

  override def +=(kv: (K, V)): this.type = {
    caches.get.put(kv._1, kv._2)
    this
  }

  override def -=(key: K): this.type = {
    caches.get.invalidate(key)
    this
  }

  override def empty: ThreadLocalCache[K, V] = new ThreadLocalCache[K, V](expiry)

  override def iterator: Iterator[(K, V)] = caches.get.asMap.asScala.iterator

  override def run(): Unit = {
    val iter = references.iterator()
    while (iter.hasNext) {
      val cache = iter.next.get
      if (cache == null) {
        // cache has been GC'd, remove our reference to it
        iter.remove()
      } else {
        cache.cleanUp()
      }
    }
  }

  override def close(): Unit = {
    cleanup.cancel(true)
    val iter = references.iterator()
    while (iter.hasNext) {
      val cache = iter.next.get
      if (cache != null) {
        cache.asMap().clear()
      }
      iter.remove()
    }
  }
}

object ThreadLocalCache {
  // use a 2 thread executor service for all the caches - we only use a handful across the code base
  private val executor = MoreExecutors.getExitingScheduledExecutorService {
    Executors.newScheduledThreadPool(2).asInstanceOf[ScheduledThreadPoolExecutor]
  }
}
