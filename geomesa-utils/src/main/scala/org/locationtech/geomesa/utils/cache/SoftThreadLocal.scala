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

import scala.ref.SoftReference

/** A value per thread wrapped in SoftReferences, which allows it to be reclaimed by
  * garbage-collection when needed.
  */
class SoftThreadLocal[T <: AnyRef] {

  protected[cache] val cache = new ThreadLocal[SoftReference[T]]()

  /**
   * @return the value for the current thread or None
   */
  def get: Option[T] = Option(cache.get()).flatMap(_.get)

  /** Set the value for the current thread, replacing the existing value, if any.
    *
   * @param value the new value
   */
  def put(value: T): Unit = cache.set(new SoftReference[T](value))

  /**
   * Get the value for the current thread or ``op`` if none.
   *
   * @param op the updated value
   * @return the existing value or ``op``
   */
  def getOrElseUpdate(op: => T): T = get match {
    case Some(values) => values
    case None =>
      val value = op
      put(value)
      value
  }

  /**
   * Remove the value for the current thread.
   */
  def clear(): Unit = cache.remove()
}
