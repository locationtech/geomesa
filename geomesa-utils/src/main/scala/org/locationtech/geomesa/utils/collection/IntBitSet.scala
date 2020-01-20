/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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
trait IntBitSet {

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
    size(length) match {
      case 1 => new BitSet32()
      case 2 => new BitSet64()
      case 3 => new BitSet96()
      case 4 => new BitSet128()
      case 5 => new BitSet160()
      case 6 => new BitSet192()
      case n => new BitSetN(Array.fill(n)(0))
    }
  }

  /**
    * Deserialize from a kryo input stream
    *
    * @param in input
    * @param length max int that the serialized bit set was capabable of storing
    * @return
    */
  def deserialize(in: Input, length: Int): IntBitSet = {
    size(length) match {
      case 1 => new BitSet32(in.readInt())
      case 2 => new BitSet64(in.readInt(), in.readInt())
      case 3 => new BitSet96(in.readInt(), in.readInt(), in.readInt())
      case 4 => new BitSet128(in.readInt(), in.readInt(), in.readInt(), in.readInt())
      case 5 => new BitSet160(in.readInt(), in.readInt(), in.readInt(), in.readInt(), in.readInt())
      case 6 => new BitSet192(in.readInt(), in.readInt(), in.readInt(), in.readInt(), in.readInt(), in.readInt())
      case n => new BitSetN(Array.fill(n)(in.readInt()))
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
    * Checks if the mask contains the value
    *
    * @param mask mask
    * @param value value
    * @return
    */
  private def contains(mask: Int, value: Int): Boolean = (mask & (1 << value)) != 0

  /**
    * Checks if the mask contains the value, and invokes `update` on the new value if not
    *
    * @param mask mask
    * @param value value
    * @param update callback for the updated value
    * @return
    */
  private def add(mask: Int, value: Int, update: Int => Unit): Boolean = {
    val updated = mask | (1 << value)
    if (updated == mask) { false } else {
      update(updated)
      true
    }
  }

  /**
    * Checks if the mask contains teh value, and invokes `update` if it does
    *
    * @param mask mask
    * @param value value
    * @param update callback for the updated value
    * @return
    */
  private def remove(mask: Int, value: Int, update: Int => Unit): Boolean = {
    val updated = mask & ~(1 << value)
    if (updated == mask) { false } else {
      update(updated)
      true
    }
  }

  /**
    * Bit set for tracking values < 32
    *
    * @param mask0 underlying masked int
    */
  class BitSet32(private var mask0: Int = 0) extends IntBitSet {

    override def contains(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.contains(mask0, value)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def add(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.add(mask0, value, updated => mask0 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def remove(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.remove(mask0, value, updated => mask0 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def clear(): Unit = mask0 = 0

    override def serialize(output: Output): Unit = output.writeInt(mask0)
  }

  /**
    * Bit set for tracking values < 64
    *
    * @param mask0 underlying masked int for 0-31
    * @param mask1 underlying masked int for 31-63
    */
  class BitSet64(private var mask0: Int = 0, private var mask1: Int = 0) extends IntBitSet {

    override def contains(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.contains(mask0, value)
        case 1 => IntBitSet.contains(mask1, value)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def add(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.add(mask0, value, updated => mask0 = updated)
        case 1 => IntBitSet.add(mask1, value, updated => mask1 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def remove(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.remove(mask0, value, updated => mask0 = updated)
        case 1 => IntBitSet.remove(mask1, value, updated => mask1 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
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
    * Bit set for tracking values < 96
    *
    * @param mask0 underlying masked int for 0-31
    * @param mask1 underlying masked int for 31-63
    * @param mask2 underlying masked int for 64-95
    */
  class BitSet96(
      private var mask0: Int = 0,
      private var mask1: Int = 0,
      private var mask2: Int = 0
    ) extends IntBitSet {

    override def contains(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.contains(mask0, value)
        case 1 => IntBitSet.contains(mask1, value)
        case 2 => IntBitSet.contains(mask2, value)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def add(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.add(mask0, value, updated => mask0 = updated)
        case 1 => IntBitSet.add(mask1, value, updated => mask1 = updated)
        case 2 => IntBitSet.add(mask2, value, updated => mask2 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def remove(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.remove(mask0, value, updated => mask0 = updated)
        case 1 => IntBitSet.remove(mask1, value, updated => mask1 = updated)
        case 2 => IntBitSet.remove(mask2, value, updated => mask2 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def clear(): Unit = {
      mask0 = 0
      mask1 = 0
      mask2 = 0
    }

    override def serialize(output: Output): Unit = {
      output.writeInt(mask0)
      output.writeInt(mask1)
      output.writeInt(mask2)
    }
  }

  /**
    * Bit set for tracking values < 128
    *
    * @param mask0 underlying masked int for 0-31
    * @param mask1 underlying masked int for 31-63
    * @param mask2 underlying masked int for 64-95
    * @param mask3 underlying masked int for 96-127
    */
  class BitSet128(
      private var mask0: Int = 0,
      private var mask1: Int = 0,
      private var mask2: Int = 0,
      private var mask3: Int = 0
    ) extends IntBitSet {

    override def contains(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.contains(mask0, value)
        case 1 => IntBitSet.contains(mask1, value)
        case 2 => IntBitSet.contains(mask2, value)
        case 3 => IntBitSet.contains(mask3, value)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def add(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.add(mask0, value, updated => mask0 = updated)
        case 1 => IntBitSet.add(mask1, value, updated => mask1 = updated)
        case 2 => IntBitSet.add(mask2, value, updated => mask2 = updated)
        case 3 => IntBitSet.add(mask3, value, updated => mask3 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def remove(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.remove(mask0, value, updated => mask0 = updated)
        case 1 => IntBitSet.remove(mask1, value, updated => mask1 = updated)
        case 2 => IntBitSet.remove(mask2, value, updated => mask2 = updated)
        case 3 => IntBitSet.remove(mask3, value, updated => mask3 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def clear(): Unit = {
      mask0 = 0
      mask1 = 0
      mask2 = 0
      mask3 = 0
    }

    override def serialize(output: Output): Unit = {
      output.writeInt(mask0)
      output.writeInt(mask1)
      output.writeInt(mask2)
      output.writeInt(mask3)
    }
  }

  /**
    * Bit set for tracking values < 160
    *
    * @param mask0 underlying masked int for 0-31
    * @param mask1 underlying masked int for 31-63
    * @param mask2 underlying masked int for 64-95
    * @param mask3 underlying masked int for 96-127
    * @param mask4 underlying masked int for 128-159
    */
  class BitSet160(
      private var mask0: Int = 0,
      private var mask1: Int = 0,
      private var mask2: Int = 0,
      private var mask3: Int = 0,
      private var mask4: Int = 0
    ) extends IntBitSet {

    override def contains(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.contains(mask0, value)
        case 1 => IntBitSet.contains(mask1, value)
        case 2 => IntBitSet.contains(mask2, value)
        case 3 => IntBitSet.contains(mask3, value)
        case 4 => IntBitSet.contains(mask4, value)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def add(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.add(mask0, value, updated => mask0 = updated)
        case 1 => IntBitSet.add(mask1, value, updated => mask1 = updated)
        case 2 => IntBitSet.add(mask2, value, updated => mask2 = updated)
        case 3 => IntBitSet.add(mask3, value, updated => mask3 = updated)
        case 4 => IntBitSet.add(mask4, value, updated => mask4 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def remove(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.remove(mask0, value, updated => mask0 = updated)
        case 1 => IntBitSet.remove(mask1, value, updated => mask1 = updated)
        case 2 => IntBitSet.remove(mask2, value, updated => mask2 = updated)
        case 3 => IntBitSet.remove(mask3, value, updated => mask3 = updated)
        case 4 => IntBitSet.remove(mask4, value, updated => mask4 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def clear(): Unit = {
      mask0 = 0
      mask1 = 0
      mask2 = 0
      mask3 = 0
      mask4 = 0
    }

    override def serialize(output: Output): Unit = {
      output.writeInt(mask0)
      output.writeInt(mask1)
      output.writeInt(mask2)
      output.writeInt(mask3)
      output.writeInt(mask4)
    }
  }

  /**
    * Bit set for tracking values < 192
    *
    * @param mask0 underlying masked int for 0-31
    * @param mask1 underlying masked int for 31-63
    * @param mask2 underlying masked int for 64-95
    * @param mask3 underlying masked int for 96-127
    * @param mask4 underlying masked int for 128-159
    * @param mask5 underlying masked int for 160-191
    */
  class BitSet192(
      private var mask0: Int = 0,
      private var mask1: Int = 0,
      private var mask2: Int = 0,
      private var mask3: Int = 0,
      private var mask4: Int = 0,
      private var mask5: Int = 0
  ) extends IntBitSet {

    override def contains(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.contains(mask0, value)
        case 1 => IntBitSet.contains(mask1, value)
        case 2 => IntBitSet.contains(mask2, value)
        case 3 => IntBitSet.contains(mask3, value)
        case 4 => IntBitSet.contains(mask4, value)
        case 5 => IntBitSet.contains(mask5, value)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def add(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.add(mask0, value, updated => mask0 = updated)
        case 1 => IntBitSet.add(mask1, value, updated => mask1 = updated)
        case 2 => IntBitSet.add(mask2, value, updated => mask2 = updated)
        case 3 => IntBitSet.add(mask3, value, updated => mask3 = updated)
        case 4 => IntBitSet.add(mask4, value, updated => mask4 = updated)
        case 5 => IntBitSet.add(mask5, value, updated => mask5 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def remove(value: Int): Boolean = {
      value >> Divisor match {
        case 0 => IntBitSet.remove(mask0, value, updated => mask0 = updated)
        case 1 => IntBitSet.remove(mask1, value, updated => mask1 = updated)
        case 2 => IntBitSet.remove(mask2, value, updated => mask2 = updated)
        case 3 => IntBitSet.remove(mask3, value, updated => mask3 = updated)
        case 4 => IntBitSet.remove(mask4, value, updated => mask4 = updated)
        case 5 => IntBitSet.remove(mask5, value, updated => mask5 = updated)
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }

    override def clear(): Unit = {
      mask0 = 0
      mask1 = 0
      mask2 = 0
      mask3 = 0
      mask4 = 0
      mask5 = 0
    }

    override def serialize(output: Output): Unit = {
      output.writeInt(mask0)
      output.writeInt(mask1)
      output.writeInt(mask2)
      output.writeInt(mask3)
      output.writeInt(mask4)
      output.writeInt(mask5)
    }
  }

  /**
    * Bit set for tracking any number of values
    *
    * @param masks underlying mask values
    */
  class BitSetN(masks: Array[Int]) extends IntBitSet {

    override def contains(value: Int): Boolean = (masks(value >> Divisor) & (1 << value)) != 0

    override def add(value: Int): Boolean = {
      val word = value >> Divisor
      IntBitSet.add(masks(word), value, updated => masks(word) = updated)
    }

    override def remove(value: Int): Boolean = {
      val word = value >> Divisor
      IntBitSet.remove(masks(word), value, updated => masks(word) = updated)
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
