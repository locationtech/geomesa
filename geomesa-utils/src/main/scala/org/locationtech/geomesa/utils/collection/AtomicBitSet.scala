/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import java.util.concurrent.atomic.AtomicIntegerArray

/**
  * Thread safe bit set. Each operation is atomic, in that a call to 'add' or 'remove' will only return
  * true exactly once (unless the value is subsequently added or removed)
  *
  * Note: we use an IntArray instead of LongArray to hopefully cause less collisions
  *
  * @param array underlying data array
  */
class AtomicBitSet(array: AtomicIntegerArray) {

  import AtomicBitSet.Divisor

  /**
    * Checks whether the value is contained in the set or not
    *
    * @param value value to check
    * @return true if contains, false otherwise
    */
  def contains(value: Int): Boolean = (array.get(value >> Divisor) & (1 << value)) != 0L

  /**
    * Adds the value to the set, if it is not present
    *
    * @param value value to add
    * @return true if value was added, false if value was already present
    */
  def add(value: Int): Boolean = {
    val word = value >> Divisor
    while (true) {
      val current = array.get(word)
      val updated = current | (1 << value)
      if (updated == current) {
        return false
      } else if (array.compareAndSet(word, current, updated)) {
        return true
      }
    }
    false // note: code will never reach here but compiler doesn't seem to realize
  }

  /**
    * Removes the value from the set, if it is present
    *
    * @param value value to remove
    * @return true if value was removed, false if value was not present
    */
  def remove(value: Int): Boolean = {
    val word = value >> Divisor
    while (true) {
      val current = array.get(word)
      val updated = current & ~(1 << value)
      if (updated == current) {
        return false
      } else if (array.compareAndSet(word, current, updated)) {
        return true
      }
    }
    false // note: code will never reach here but compiler doesn't seem to realize
  }

  /**
    * Resets all values
    */
  def clear(): Unit = {
    var i = 0
    while (i < array.length) {
      array.set(i, 0)
      i += 1
    }
  }
}

object AtomicBitSet {

  // use `>> 5` instead of `/ 32`
  private final val Divisor = 5

  def apply(length: Int): AtomicBitSet = new AtomicBitSet(new AtomicIntegerArray(((length - 1) >> Divisor) + 1))
}