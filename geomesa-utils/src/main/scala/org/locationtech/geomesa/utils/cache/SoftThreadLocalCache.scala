/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import scala.ref.SoftReference

/**
 * Creates a per-thread cache of values wrapped in SoftReferences, which allows them to be reclaimed
 * by garbage-collection when needed.
 *
 * @tparam K key type
 * @tparam V value type
 */
class SoftThreadLocalCache[K, V <: AnyRef] extends scala.collection.mutable.Map[K, V]
    with scala.collection.mutable.MapLike[K, V, SoftThreadLocalCache[K, V]] {

  protected [cache] val cache = new ThreadLocal[scala.collection.mutable.Map[K, SoftReference[V]]] {
    override def initialValue: scala.collection.mutable.Map[K, SoftReference[V]] =
      scala.collection.mutable.Map.empty
  }

  override def get(key: K): Option[V] = cache.get().get(key).flatMap(_.get)

  override def getOrElseUpdate(key: K, op: => V): V = get(key) match {
    case Some(values) => values
    case None =>
      val values = op
      cache.get().put(key, new SoftReference[V](values))
      values
  }

  override def +=(kv: (K, V)): this.type = {
    cache.get() += ((kv._1, new SoftReference[V](kv._2)))
    this
  }

  override def -=(key: K): this.type = {
    cache.get().remove(key).flatMap(_.get)
    this
  }

  override def empty: SoftThreadLocalCache[K, V] = new SoftThreadLocalCache[K, V]()

  override def iterator: Iterator[(K, V)] =
    cache.get().iterator.withFilter(_._2.get.isDefined).map { case (k, v) => (k, v.get.get) }
}
