/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.InputStream

import org.locationtech.geomesa.utils.collection.CircularByteQueue

/**
  * Proxies an input stream, copying any bytes read. The number of copied bytes can be checked with `copied`,
  * discarded with `drop`, and returned with `replay` (which will also drop the bytes).
  *
  * Note: this class is not thread-safe
  *
  * @param wrapped input stream to proxy
  * @param initialBuffer initial size of the array allocated for copying bytes on read. The array will grow as needed
  */
class CopyingInputStream(wrapped: InputStream, initialBuffer: Int = 16) extends InputStream {

  // circular buffer holding our copied bytes
  private val copy = new CircularByteQueue(initialBuffer)
  private var skipped = Array.empty[Byte]

  /**
    * The number of bytes currently available for replaying from the underlying stream
    *
    * @return
    */
  def copied: Int = copy.size

  /**
    * Discard bytes that have been copied from the underlying stream
    *
    * @param count number of bytes to discard
    */
  def drop(count: Int): Unit = copy.drop(count)

  /**
    * Return bytes copied from the underlying stream, discarding them afterwards. If more bytes are requested
    * than have been copied, only copied bytes will be returned
    *
    * @param count number of bytes to return
    * @return
    */
  def replay(count: Int): Array[Byte] = copy.dequeue(count)

  override def read(): Int = {
    val c = wrapped.read()
    if (c != -1) {
      copy.enqueue(c.toByte)
    }
    c
  }

  override def read(b: Array[Byte]): Int = {
    val count = wrapped.read(b)
    if (count != -1) {
      copy.enqueue(b, 0, count)
    }
    count
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val count = wrapped.read(b, off, len)
    if (count != -1) {
      copy.enqueue(b, off, count)
    }
    count
  }

  override def skip(n: Long): Long = {
    if (n > Int.MaxValue) {
      var remaining = n
      var count = 0
      var total = 0L
      while (remaining > 0) {
        val toRead = math.min(remaining, Int.MaxValue.toLong).toInt
        remaining -= toRead
        if (toRead > skipped.length) {
          skipped = Array.ofDim(toRead)
        }
        count = wrapped.read(skipped, 0, toRead)
        if (count != -1) {
          copy.enqueue(skipped, 0, count)
          total += count.toLong
        } else {
          remaining = 0
        }
      }
      // release the byte buffer, as it is now Int.MaxValue length
      skipped = Array.empty
      total
    } else {
      if (n > skipped.length) {
        skipped = Array.ofDim(n.toInt)
      }
      val count = wrapped.read(skipped, 0, n.toInt)
      if (count != -1) {
        copy.enqueue(skipped, 0, count)
      }
      count.toLong
    }
  }

  override def available(): Int = wrapped.available()

  // note: mark/reset not supported, defer to default implementation that throws IllegalArgumentExceptions

  override def close(): Unit = wrapped.close()
}
