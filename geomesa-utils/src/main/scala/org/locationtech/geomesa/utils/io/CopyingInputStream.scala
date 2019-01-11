/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.{ByteArrayOutputStream, InputStream}

/**
  * Proxies an input stream, copying any bytes read to `copy`
  *
  * @param in input stream to proxy
  */
class CopyingInputStream(in: InputStream) extends InputStream {

  val copy: ByteArrayOutputStream = new ByteArrayOutputStream()

  private var skipped = Array.empty[Byte]

  override def read(): Int = {
    val c = in.read()
    if (c != -1) {
      copy.write(c)
    }
    c
  }

  override def read(b: Array[Byte]): Int = {
    val count = in.read(b)
    if (count != -1) {
      copy.write(b, 0, count)
    }
    count
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val count = in.read(b, off, len)
    if (count != -1) {
      copy.write(b, off, count)
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
        count = in.read(skipped, 0, toRead)
        if (count != -1) {
          copy.write(skipped, 0, count)
          total += count.toLong
        } else {
          remaining = 0
        }
      }
      // release the byte buffer, as it is Int.MaxValue length
      skipped = Array.empty
      total
    } else {
      if (n > skipped.length) {
        skipped = Array.ofDim(n.toInt)
      }
      val count = in.read(skipped, 0, n.toInt)
      if (count != -1) {
        copy.write(skipped, 0, count)
      }
      count.toLong
    }
  }

  override def available(): Int = in.available()

  override def markSupported(): Boolean = in.markSupported()

  override def mark(readlimit: Int): Unit = in.mark(readlimit)

  override def reset(): Unit = in.reset()

  override def close(): Unit = in.close()
}
