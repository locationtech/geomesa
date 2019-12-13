/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

/**
  * Provides synchronized or unsynchronized access to an underlying object. The main use case is to transition to
  * a final desired state - if our initial state is our desired state, we can totally avoid synchronization.
  *
  * If the object is going to be updated, use `IsSynchronized` access. If the value is read-only,
  * use `NotSynchronized` access.
  *
  * @tparam T type parameter
  */
trait MaybeSynchronized[T] {

  /**
    * Gets the current value
    *
    * @return
    */
  def get: T

  /**
    * Atomic operation to conditionally set the current value. If the current value matches the expected value
    * passed in, 'onMatch' will be executed and the current value will be updated. Otherwise, nothing will happen.
    *
    * @param value value to set
    * @param expected expected current value
    * @param onMatch will be executed if current value == expected
    * @return true if match was excuted
    */
  def set(value: T, expected: T, onMatch: => Unit = {}): Boolean
}

/**
  * Access to the underlying object is synchronized. Supports both get and set
  *
  * @param initial initial value for the underlying object
  * @tparam T type parameter
  */
class IsSynchronized[T](initial: T) extends MaybeSynchronized[T] {
  private var current = initial
  override def get: T = synchronized(current)
  override def set(value: T, expected: T, onMatch: => Unit): Boolean = synchronized {
    if (current == expected) {
      onMatch
      current = value
      true
    } else {
      false
    }
  }
}

/**
  * Access to the underlying value is not synchronized. Supports get but does not support set
  *
  * @param value underlying value
  * @tparam T type parameter
  */
class NotSynchronized[T](value: T) extends MaybeSynchronized[T] {
  override def get: T = value
  override def set(ignored: T, expected: T, f: => Unit): Boolean =
    if (value == expected) {
      throw new NotImplementedError("NotSynchronized is read-only")
    } else {
      false
    }
}
