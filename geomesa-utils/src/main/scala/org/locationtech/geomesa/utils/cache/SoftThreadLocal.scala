/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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
