/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.utils.cache

import scala.collection.mutable.{Map, MapLike}
import scala.ref.SoftReference

/**
 * Creates a per-thread cache of values wrapped in SoftReferences, which allows them to be reclaimed
 * by garbage-collection when needed.
 *
 * @tparam K
 * @tparam V
 */
class SoftThreadLocalCache[K, V <: AnyRef] extends Map[K, V] with MapLike[K, V, SoftThreadLocalCache[K, V]] {

  protected[cache] val cache = new ThreadLocal[Map[K, SoftReference[V]]] {
    override def initialValue = Map.empty
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
