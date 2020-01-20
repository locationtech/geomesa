/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import org.locationtech.geomesa.utils.collection.WordBitSet.Word

/**
  * A bit set backed by ints (to reduce memory footprint for small sizes).
  *
  * Provides access to the underlying words, for low-level synchronization. Not thread safe.
  */
trait WordBitSet {

  /**
    * Gets the word for the given value. The word can be synchronized on for any operations on the given value.
    *
    * @param value value
    * @return word
    */
  def word(value: Int): Word
}

object WordBitSet {

  // use `>> 5` instead of `/ 32`
  private final val Divisor = 5

  /**
    * Create a bit set capable of holding numbers up to `length`
    *
    * @param length max int to be stored in the bit set
    * @return
    */
  def apply(length: Int): WordBitSet = {
    IntBitSet.size(length) match {
      case 1 => new WordBitSet32()
      case 2 => new WordBitSet64()
      case 3 => new WordBitSet96()
      case 4 => new WordBitSet128()
      case 5 => new WordBitSet160()
      case 6 => new WordBitSet192()
      case n => new WordBitSetN(n)
    }
  }

  /**
    * A single word in the bit set
    *
    * @param mask the mask for this word
    */
  class Word(private var mask: Int = 0) {

    /**
      * Checks whether the value is contained in the set or not
      *
      * @param value value to check
      * @return true if contains, false otherwise
      */
    def contains(value: Int): Boolean = (mask & (1 << value)) != 0

    /**
      * Adds the value to the set, if it is not present. Note that implementations may be constructed with
      * a fixed capacity - attempting to add a value outside the capacity will result in undefined behavior
      *
      * @param value value to add
      * @return true if value was added, false if value was already present
      */
    def add(value: Int): Boolean = {
      val updated = mask | (1 << value)
      if (updated == mask) { false } else {
        mask = updated
        true
      }
    }

    /**
      * Removes the value from the set, if it is present
      *
      * @param value value to remove
      * @return true if value was removed, false if value was not present
      */
    def remove(value: Int): Boolean = {
      val updated = mask & ~(1 << value)
      if (updated == mask) { false } else {
        mask = updated
        true
      }
    }
  }

  /**
    * Bit set for tracking values < 32
    */
  class WordBitSet32 extends WordBitSet {

    private val word0 = new Word()

    override def word(value: Int): Word =  {
      value >> Divisor match {
        case 0 => word0
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }
  }

  /**
    * Bit set for tracking values < 64
    */
  class WordBitSet64 extends WordBitSet {

    private val word0 = new Word()
    private val word1 = new Word()

    override def word(value: Int): Word = {
      value >> Divisor match {
        case 0 => word0
        case 1 => word1
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }
  }

  /**
    * Bit set for tracking values < 96
    */
  class WordBitSet96 extends WordBitSet {

    private val word0 = new Word()
    private val word1 = new Word()
    private val word2 = new Word()

    override def word(value: Int): Word = {
      value >> Divisor match {
        case 0 => word0
        case 1 => word1
        case 2 => word2
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }
  }

  /**
    * Bit set for tracking values < 128
    */
  class WordBitSet128 extends WordBitSet {

    private val word0 = new Word()
    private val word1 = new Word()
    private val word2 = new Word()
    private val word3 = new Word()

    override def word(value: Int): Word = {
      value >> Divisor match {
        case 0 => word0
        case 1 => word1
        case 2 => word2
        case 3 => word3
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }
  }

  /**
    * Bit set for tracking values < 160
    */
  class WordBitSet160 extends WordBitSet {

    private val word0 = new Word()
    private val word1 = new Word()
    private val word2 = new Word()
    private val word3 = new Word()
    private val word4 = new Word()

    override def word(value: Int): Word = {
      value >> Divisor match {
        case 0 => word0
        case 1 => word1
        case 2 => word2
        case 3 => word3
        case 4 => word4
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }
  }

  /**
    * Bit set for tracking values < 192
    */
  class WordBitSet192 extends WordBitSet {

    private val word0 = new Word()
    private val word1 = new Word()
    private val word2 = new Word()
    private val word3 = new Word()
    private val word4 = new Word()
    private val word5 = new Word()

    override def word(value: Int): Word = {
      value >> Divisor match {
        case 0 => word0
        case 1 => word1
        case 2 => word2
        case 3 => word3
        case 4 => word4
        case 5 => word5
        case _ => throw new ArrayIndexOutOfBoundsException(value)
      }
    }
  }

  /**
    * Bit set for tracking any number of values
    *
    * @param n number of words
    */
  class WordBitSetN(n: Int) extends WordBitSet {

    private val words = Array.fill(n)(new Word())

    override def word(value: Int): Word = words(value >> Divisor)
  }
}
