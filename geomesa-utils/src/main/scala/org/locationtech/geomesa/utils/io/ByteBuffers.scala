/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object ByteBuffers {

  class ExpandingByteBuffer(capacity: Int) {

    private var bb = ByteBuffer.allocate(capacity)

    private def ensureRemaining(count: Int): Unit = {
      if (bb.remaining < count) {
        val expanded = ByteBuffer.allocate(bb.capacity() * 2)
        bb.flip()
        expanded.put(bb)
        bb = expanded
      }
    }

    def putString(string: String): Unit = {
      if (string == null) { putInt(-1) } else {
        putBytes(string.getBytes(StandardCharsets.UTF_8))
      }
    }
    def getString: String = {
      val length = getInt
      if (length == -1) { null } else {
        val bytes = Array.ofDim[Byte](length)
        bb.get(bytes)
        new String(bytes, StandardCharsets.UTF_8)
      }
    }

    def putBytes(bytes: Array[Byte]): Unit = { ensureRemaining(bytes.length + 4); bb.putBytes(bytes) }
    def getBytes: Array[Byte] = bb.getBytes

    def putBool(bool: Boolean): Unit = { ensureRemaining(1); bb.putBool(bool) }
    def getBool: Boolean = bb.getBool

    def toArray: Array[Byte] = bb.toArray

    def put(b: Byte): ByteBuffer = { ensureRemaining(1); bb.put(b) }
    def get(): Byte = bb.get()

    def put(src: Array[Byte]): ByteBuffer = { ensureRemaining(src.length); bb.put(src) }
    def get(dst: Array[Byte]): ByteBuffer = bb.get(dst)

    def putChar(value: Char): ByteBuffer = { ensureRemaining(2); bb.putChar(value) }
    def getChar: Char = bb.getChar

    def putShort(value: Short): ByteBuffer = { ensureRemaining(2); bb.putShort(value) }
    def getShort: Short = bb.getShort

    def putInt(value: Int): ByteBuffer = { ensureRemaining(4); bb.putInt(value) }
    def getInt: Int = bb.getInt

    def putLong(value: Long): ByteBuffer = { ensureRemaining(8); bb.putLong(value) }
    def getLong: Long = bb.getLong

    def putFloat(value: Float): ByteBuffer = { ensureRemaining(4); bb.putFloat(value) }
    def getFloat: Float = bb.getFloat

    def putDouble(value: Double): ByteBuffer = { ensureRemaining(8); bb.putDouble(value) }
    def getDouble: Double = bb.getDouble
  }

  implicit class RichByteBuffer(val bb: ByteBuffer) extends AnyVal {

    def toInputStream: InputStream = new ByteBufferInputStream(bb)

    def putBytes(bytes: Array[Byte]): Unit = {
      bb.putInt(bytes.length)
      bb.put(bytes)
    }
    def getBytes: Array[Byte] = {
      val bytes = Array.ofDim[Byte](bb.getInt())
      bb.get(bytes)
      bytes
    }

    def putString(string: String): Unit = {
      if (string == null) { bb.putInt(-1) } else {
        putBytes(string.getBytes(StandardCharsets.UTF_8))
      }
    }
    def getString: String = {
      val length = bb.getInt
      if (length == -1) { null } else {
        val bytes = Array.ofDim[Byte](length)
        bb.get(bytes)
        new String(bytes, StandardCharsets.UTF_8)
      }
    }

    def putBool(bool: Boolean): Unit = bb.put(if (bool) { 1.toByte } else { 0.toByte })
    def getBool: Boolean = bb.get == 1

    def toArray: Array[Byte] = {
      bb.flip()
      val bytes = Array.ofDim[Byte](bb.remaining)
      bb.get(bytes)
      bytes
    }
  }

  class ByteBufferInputStream(buffer: ByteBuffer) extends InputStream {

    override def read(): Int = {
      if (!buffer.hasRemaining) { -1 } else {
        buffer.get() & 0xFF
      }
    }

    override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
      if (!buffer.hasRemaining) { -1 } else {
        val read = math.min(length, buffer.remaining)
        buffer.get(bytes, offset, read)
        read
      }
    }
  }

}
