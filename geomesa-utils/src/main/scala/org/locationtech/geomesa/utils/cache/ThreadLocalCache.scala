/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import java.io.Closeable
import java.lang.ref.WeakReference
import java.util.concurrent._

import com.github.benmanes.caffeine.cache.{Cache, Caffeine, Ticker}
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor

import scala.concurrent.duration.Duration

/**
 * Creates a per-thread cache of values with a timed expiration.
 *
 * Map operations will only affect/reflect the state of the current thread. Additional methods `globalIterator`
 * and `estimatedGlobalSize` are provided for global views across all threads, which generally should only be used
 * for debugging.
 *
 * Since caches are only cleaned up when accessed, uses an asynchronous thread to
 * actively clean up any orphaned thread local values
 *
 * @tparam K key type
 * @tparam V value type
 */
class ThreadLocalCache[K <: AnyRef, V <: AnyRef](
    expiry: Duration,
    executor: ScheduledExecutorService = ThreadLocalCache.executor,
    ticker: Option[Ticker] = None
  ) extends scala.collection.mutable.Map[K, V]
    with scala.collection.mutable.MapLike[K, V, ThreadLocalCache[K, V]]
    with Runnable
    with Closeable {

  import scala.collection.JavaConverters._

  // weak references to our current caches, to allow cleanup + GC
  private val references = new ConcurrentLinkedQueue[(Long, WeakReference[Cache[K, V]])]()

  private val caches = new ThreadLocal[Cache[K, V]]() {
    override def initialValue(): Cache[K, V] = {
      val builder = Caffeine.newBuilder().expireAfterAccess(expiry.toMillis, TimeUnit.MILLISECONDS)
      ticker.foreach(builder.ticker)
      val cache = builder.build[K, V]()
      // this will always succeed as our queue is unbounded
      references.offer((Thread.currentThread().getId, new WeakReference(cache)))
      cache
    }
  }

  private val cleanup = executor.scheduleWithFixedDelay(this, expiry.toMillis, expiry.toMillis, TimeUnit.MILLISECONDS)

  // show the class name for toString
  override def stringPrefix : String = "ThreadLocalCache"

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

  /**
   * Gets an iterator across all thread-local values, not just the current thread
   *
   * @return iterator of (thread-id, key, value)
   */
  def globalIterator: Iterator[(Long, K, V)] = {
    references.iterator.asScala.flatMap { case (id, ref) =>
      val cache = ref.get
      if (cache == null) { Iterator.empty } else {
        cache.asMap().asScala.iterator.map { case (k, v) => (id, k, v) }
      }
    }
  }

  /**
   * Gets the estimated total size across all thread-local values
   *
   * @return
   */
  def estimatedGlobalSize: Long = {
    var size = 0L
    references.iterator.asScala.foreach { case (_, ref) =>
      val cache = ref.get
      if (cache != null) {
        size += cache.estimatedSize()
      }
    }
    size
  }

  override def run(): Unit = {
    val iter = references.iterator()
    while (iter.hasNext) {
      val cache = iter.next._2.get
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
      val cache = iter.next._2.get
      if (cache != null) {
        cache.asMap().clear()
      }
      iter.remove()
    }
  }
}

object ThreadLocalCache {
  // use a 2 thread executor service for all the caches - we only use a handful across the code base
  private val executor = ExitingExecutor(Executors.newScheduledThreadPool(2).asInstanceOf[ScheduledThreadPoolExecutor])
}
