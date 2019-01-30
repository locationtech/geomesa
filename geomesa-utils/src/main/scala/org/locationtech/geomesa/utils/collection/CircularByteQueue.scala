/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

/**
  * Growable byte queue that holds values in a circular array to minimize allocations
  *
  * @param initialSize initial buffer size
  */
class CircularByteQueue(initialSize: Int = 16) {

  private var array = Array.ofDim[Byte](initialSize)
  private var start = 0
  private var length = 0

  /**
    * Number of bytes stored in this buffer
    *
    * @return
    */
  def size: Int = length

  /**
    * Current remaining capacity. In general this is not relevant, as the queue will grow as needed,
    * but it can be useful for debugging
    *
    * @return current capacity of the buffer
    */
  def capacity: Int = array.length - length

  /**
    * Enqueue a single byte
    *
    * @param byte byte
    */
  def += (byte: Byte): Unit = enqueue(byte)

  /**
    * Enqueue multiple bytes
    *
    * @param bytes bytes
    */
  def ++= (bytes: Array[Byte]): Unit = enqueue(bytes, 0, bytes.length)

  /**
    * Enqueue a single byte
    *
    * @param byte byte
    */
  def enqueue(byte: Byte): Unit = {
    ensure(1)
    var last = start + length
    if (last >= array.length) {
      last -= array.length
    }
    array(last) = byte
    length += 1
  }

  /**
    * Enqueue multiple bytes
    *
    * @param bytes bytes
    * @param offset offset into the array to start reading, must be valid for the bytes passed in
    * @param count number of bytes to enqueue, must be valid for the size of the bytes and offset passed in
    */
  def enqueue(bytes: Array[Byte], offset: Int, count: Int): Unit = {
    ensure(count)
    var first = start + length
    if (first >= array.length) {
      first -= array.length
    }
    if (first + count < array.length) {
      System.arraycopy(bytes, offset, array, first, count)
    } else {
      val tail = array.length - first
      System.arraycopy(bytes, offset, array, first, tail)
      System.arraycopy(bytes, offset + tail, array, 0, count - tail)
    }
    length += count
  }

  /**
    * Dequeue bytes
    *
    * @param count number of bytes to remove
    * @return
    */
  def dequeue(count: Int): Array[Byte] = {
    val total = math.min(length, count)
    val result = Array.ofDim[Byte](total)
    if (start + total <= array.length) {
      System.arraycopy(array, start, result, 0, total)
    } else {
      val tail = array.length - start
      System.arraycopy(array, start, result, 0, tail)
      System.arraycopy(array, 0, result, tail, total - tail)
    }
    start += total
    if (start >= array.length) {
      start -= array.length
    }
    length -= total
    result
  }

  /**
    * Drop bytes
    *
    * @param count number of bytes to drop
    */
  def drop(count: Int): Unit = {
    val total = math.min(length, count)
    start += total
    if (start >= array.length) {
      start -= array.length
    }
    length -= total
  }

  /**
    * Grow the array as needed to fit an additional number of bytes
    *
    * @param count bytes to fit
    */
  private def ensure(count: Int): Unit = {
    if (length + count > array.length) {
      var newSize = array.length * 2
      while (newSize < length + count) {
        newSize = newSize * 2
      }
      val tmp = Array.ofDim[Byte](newSize)
      if (start + length <= array.length) {
        System.arraycopy(array, start, tmp, 0, length)
      } else {
        val tailBytes = array.length - start
        System.arraycopy(array, start, tmp, 0, tailBytes)
        System.arraycopy(array, 0, tmp, tailBytes, length - tailBytes)
      }
      array = tmp
      start = 0
    }
  }
}