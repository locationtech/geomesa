/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import com.esotericsoftware.kryo.io.{Input, Output}

/**
  * A bit set backed by ints (to reduce memory footprint for small sizes).
  *
  * Provides access to the underlying mask, for serialization. Not thread safe.
  */
sealed abstract class IntBitSet {

  /**
    * Checks whether the value is contained in the set or not
    *
    * @param value value to check
    * @return true if contains, false otherwise
    */
  def contains(value: Int): Boolean

  /**
    * Adds the value to the set, if it is not present. Note that implementations may be constructed with
    * a fixed capacity - attempting to add a value outside the capacity will result in undefined behavior
    *
    * @param value value to add
    * @return true if value was added, false if value was already present
    */
  def add(value: Int): Boolean

  /**
    * Removes the value from the set, if it is present
    *
    * @param value value to remove
    * @return true if value was removed, false if value was not present
    */
  def remove(value: Int): Boolean

  /**
    * Resets all values
    */
  def clear(): Unit

  /**
    * Serialize to a kryo output stream
    *
    * @param output output
    */
  def serialize(output: Output): Unit
}

object IntBitSet {

  // use `>> 5` instead of `/ 32`
  private final val Divisor = 5

  /**
    * Create a bit set capable of holding numbers up to `length`
    *
    * @param length max int to be stored in the bit set
    * @return
    */
  def apply(length: Int): IntBitSet = {
    val words = size(length)
    if (words == 1) {
      new BitSet32(0)
    } else if (words == 2) {
      new BitSet64(0, 0)
    } else {
      new BitSetN(Array.fill(words)(0))
    }
  }

  /**
    * Deserialize from a kryo input stream
    *
    * @param input input
    * @param length max int that the serialized bit set was capabable of storing
    * @return
    */
  def deserialize(input: Input, length: Int): IntBitSet = {
    val words = size(length)
    if (words == 1) {
      new BitSet32(input.readInt())
    } else if (words == 2) {
      new BitSet64(input.readInt(), input.readInt())
    } else {
      new BitSetN(Array.fill(words)(input.readInt()))
    }
  }

  /**
    * Gets the number of ints (words) required to hold numbers up to `length`
    *
    * @param length max int to be stored in the bit set
    * @return
    */
  def size(length: Int): Int = ((length - 1) >> Divisor) + 1

  /**
    * Bit set for tracking values < 32
    *
    * @param mask underlying masked int
    */
  class BitSet32(private var mask: Int) extends IntBitSet {

    override def contains(value: Int): Boolean = (mask & (1 << value)) != 0

    override def add(value: Int): Boolean = {
      val updated = mask | (1 << value)
      if (updated == mask) {
        false
      } else {
        mask = updated
        true
      }
    }

    override def remove(value: Int): Boolean = {
      val updated = mask & ~(1 << value)
      if (updated == mask) {
        false
      } else {
        mask = updated
        true
      }
    }

    override def clear(): Unit = mask = 0

    override def serialize(output: Output): Unit = output.writeInt(mask)
  }

  /**
    * Bit set for tracking values < 64
    *
    * @param mask0 underlying masked int for 0-31
    * @param mask1 underlying masked int for 31-63
    */
  class BitSet64(private var mask0: Int, private var mask1: Int) extends IntBitSet {

    override def contains(value: Int): Boolean = {
      val mask = if (value >> Divisor == 0) { mask0 } else { mask1 }
      (mask & (1 << value)) != 0
    }

    override def add(value: Int): Boolean = {
      if (value >> Divisor == 0) {
        val updated = mask0 | (1 << value)
        if (updated == mask0) {
          false
        } else {
          mask0 = updated
          true
        }
      } else {
        val updated = mask1 | (1 << value)
        if (updated == mask1) {
          false
        } else {
          mask1 = updated
          true
        }
      }
    }

    override def remove(value: Int): Boolean = {
      if (value >> Divisor == 0) {
        val updated = mask0 & ~(1 << value)
        if (updated == mask0) {
          false
        } else {
          mask0 = updated
          true
        }
      } else {
        val updated = mask1 & ~(1 << value)
        if (updated == mask1) {
          false
        } else {
          mask1 = updated
          true
        }
      }
    }

    override def clear(): Unit = {
      mask0 = 0
      mask1 = 0
    }

    override def serialize(output: Output): Unit = {
      output.writeInt(mask0)
      output.writeInt(mask1)
    }
  }

  /**
    * Bit set for tracking values >= 64
    *
    * @param masks underlying mask values
    */
  class BitSetN(masks: Array[Int]) extends IntBitSet {

    override def contains(value: Int): Boolean = (masks(value >> Divisor) & (1 << value)) != 0

    override def add(value: Int): Boolean = {
      val word = value >> Divisor
      val current = masks(word)
      val updated = current | (1 << value)
      if (updated == current) {
        false
      } else {
        masks(word) = updated
        true
      }
    }

    override def remove(value: Int): Boolean = {
      val word = value >> Divisor
      val current = masks(word)
      val updated = current & ~(1 << value)
      if (updated == current) {
        false
      } else {
        masks(word) = updated
        true
      }
    }

    override def clear(): Unit = {
      var i = 0
      while (i < masks.length) {
        masks(i) = 0
        i += 1
      }
    }

    override def serialize(output: Output): Unit = masks.foreach(output.writeInt)
  }
}
