/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/**
  * Portions
  * Copyright 2011-2016 The Apache Software Foundation
  */

package org.locationtech.geomesa.utils.index

import com.google.common.primitives.UnsignedBytes

object ByteArrays {

  val ZeroByte: Byte = 0x00.toByte
  val OneByte: Byte  = 0x01.toByte
  val MaxByte: Byte =  0xff.toByte

  val ZeroByteArray = Array(ByteArrays.ZeroByte)
  val OneByteArray = Array(ByteArrays.OneByte)

  implicit val ByteOrdering: Ordering[Array[Byte]] =
    Ordering.comparatorToOrdering(UnsignedBytes.lexicographicalComparator)

  /**
    * Writes the short as 2 bytes in the provided array, starting at offset
    *
    * @param short short to write
    * @param bytes byte array to write to, must have length at least `offset` + 2
    * @param offset offset to start writing
    */
  def writeShort(short: Short, bytes: Array[Byte], offset: Int = 0): Unit = {
    bytes(offset) = (short >> 8).asInstanceOf[Byte]
    bytes(offset + 1) = short.asInstanceOf[Byte]
  }

  /**
    * Writes the short as 2 bytes in the provided array, starting at offset,
    * and preserving sort order for negative values
    *
    * @param short short to write
    * @param bytes bytes array to write to, must have length at least `offset` + 2
    * @param offset offset to start writing
    */
  def writeOrderedShort(short: Short, bytes: Array[Byte], offset: Int = 0): Unit = {
    bytes(offset) = (((short >> 8) & 0xff) ^ 0x80).asInstanceOf[Byte]
    bytes(offset + 1) = (short & 0xff).asInstanceOf[Byte]
  }

  /**
    * Writes the int as 4 bytes in the provided array, starting at offset
    *
    * @param int int to write
    * @param bytes byte array to write to, must have length at least `offset` + 8
    * @param offset offset to start writing
    */
  def writeInt(int: Int, bytes: Array[Byte], offset: Int = 0): Unit = {
    bytes(offset    ) = ((int >> 24) & 0xff).asInstanceOf[Byte]
    bytes(offset + 1) = ((int >> 16) & 0xff).asInstanceOf[Byte]
    bytes(offset + 2) = ((int >> 8)  & 0xff).asInstanceOf[Byte]
    bytes(offset + 3) =  (int        & 0xff).asInstanceOf[Byte]
  }

  /**
    * Writes the long as 8 bytes in the provided array, starting at offset
    *
    * @param long long to write
    * @param bytes byte array to write to, must have length at least `offset` + 8
    * @param offset offset to start writing
    */
  def writeLong(long: Long, bytes: Array[Byte], offset: Int = 0): Unit = {
    bytes(offset    ) = ((long >> 56) & 0xff).asInstanceOf[Byte]
    bytes(offset + 1) = ((long >> 48) & 0xff).asInstanceOf[Byte]
    bytes(offset + 2) = ((long >> 40) & 0xff).asInstanceOf[Byte]
    bytes(offset + 3) = ((long >> 32) & 0xff).asInstanceOf[Byte]
    bytes(offset + 4) = ((long >> 24) & 0xff).asInstanceOf[Byte]
    bytes(offset + 5) = ((long >> 16) & 0xff).asInstanceOf[Byte]
    bytes(offset + 6) = ((long >> 8)  & 0xff).asInstanceOf[Byte]
    bytes(offset + 7) =  (long        & 0xff).asInstanceOf[Byte]
  }

  /**
    * Writes the long as 8 bytes in the provided array, starting at offset,
    * and preserving sort order for negative values
    *
    * @param long long to write
    * @param bytes bytes array to write to, must have length at least `offset` + 8
    * @param offset offset to start writing
    */
  def writeOrderedLong(long: Long, bytes: Array[Byte], offset: Int = 0): Unit = {
    bytes(offset    ) = (((long >> 56) & 0xff) ^ 0x80).asInstanceOf[Byte]
    bytes(offset + 1) = ((long >> 48) & 0xff).asInstanceOf[Byte]
    bytes(offset + 2) = ((long >> 40) & 0xff).asInstanceOf[Byte]
    bytes(offset + 3) = ((long >> 32) & 0xff).asInstanceOf[Byte]
    bytes(offset + 4) = ((long >> 24) & 0xff).asInstanceOf[Byte]
    bytes(offset + 5) = ((long >> 16) & 0xff).asInstanceOf[Byte]
    bytes(offset + 6) = ((long >> 8)  & 0xff).asInstanceOf[Byte]
    bytes(offset + 7) =  (long        & 0xff).asInstanceOf[Byte]
  }

  /**
    * Reads 2 bytes from the provided array as a short, starting at offset
    *
    * @param bytes array to read from
    * @param offset offset to start reading
    * @return
    */
  def readShort(bytes: Array[Byte], offset: Int = 0): Short =
    (((bytes(offset) & 0xff) << 8) | (bytes(offset + 1) & 0xff)).toShort

  /**
    * Reads 2 bytes from the provided array as a short, starting at offset
    *
    * @param bytes array to read from
    * @param offset offset to start reading
    * @return
    */
  def readOrderedShort(bytes: Array[Byte], offset: Int = 0): Short =
    ((((bytes(offset) ^ 0x80) & 0xff) << 8) | (bytes(offset + 1) & 0xff)).toShort

  /**
    * Reads 4 bytes from the provided array as an int, starting at offset
    *
    * @param bytes array to read from
    * @param offset offset to start reading
    * @return
    */
  def readInt(bytes: Array[Byte], offset: Int = 0): Int = {
    ((bytes(offset    ) & 0xff) << 24) |
    ((bytes(offset + 1) & 0xff) << 16) |
    ((bytes(offset + 2) & 0xff) <<  8) |
     (bytes(offset + 3) & 0xff)
  }

  /**
    * Reads 8 bytes from the provided array as a long, starting at offset
    *
    * @param bytes array to read from
    * @param offset offset to start reading
    * @return
    */
  def readLong(bytes: Array[Byte], offset: Int = 0): Long = {
    ((bytes(offset    ) & 0xffL) << 56) |
    ((bytes(offset + 1) & 0xffL) << 48) |
    ((bytes(offset + 2) & 0xffL) << 40) |
    ((bytes(offset + 3) & 0xffL) << 32) |
    ((bytes(offset + 4) & 0xffL) << 24) |
    ((bytes(offset + 5) & 0xffL) << 16) |
    ((bytes(offset + 6) & 0xffL) <<  8) |
     (bytes(offset + 7) & 0xffL)
  }

  /**
    * Reads 8 bytes from the provided array as a long, starting at offset
    *
    * @param bytes array to read from
    * @param offset offset to start reading
    * @return
    */
  def readOrderedLong(bytes: Array[Byte], offset: Int = 0): Long = {
    (((bytes(offset) ^ 0x80) & 0xffL) << 56) |
    ((bytes(offset + 1) & 0xffL) << 48) |
    ((bytes(offset + 2) & 0xffL) << 40) |
    ((bytes(offset + 3) & 0xffL) << 32) |
    ((bytes(offset + 4) & 0xffL) << 24) |
    ((bytes(offset + 5) & 0xffL) << 16) |
    ((bytes(offset + 6) & 0xffL) <<  8) |
     (bytes(offset + 7) & 0xffL)
  }

  /**
    * Allocates a new array of length two and writes the short to it
    *
    * @param short value to encode
    * @return
    */
  def toBytes(short: Short): Array[Byte] = {
    val result = Array.ofDim[Byte](2)
    writeShort(short, result)
    result
  }

  /**
    * Allocates a new array of length two and writes the short to it, preserving sort order for negative values
    *
    * @param short value to encode
    * @return
    */
  def toOrderedBytes(short: Short): Array[Byte] = {
    val result = Array.ofDim[Byte](2)
    writeOrderedShort(short, result)
    result
  }

  /**
    * Allocates a new array of length eight and writes the long to it
    *
    * @param long value to encode
    * @return
    */
  def toBytes(long: Long): Array[Byte] = {
    val result = Array.ofDim[Byte](8)
    writeLong(long, result)
    result
  }

  /**
    * Allocates a new array of length eight and writes the long to it, preserving sort order for negative values
    *
    * @param long value to encode
    * @return
    */
  def toOrderedBytes(long: Long): Array[Byte] = {
    val result = Array.ofDim[Byte](8)
    writeOrderedLong(long, result)
    result
  }

  /**
    * Creates a byte array with a short and a long.
    *
    * Code based on the following methods, but avoids allocating extra byte arrays:
    *
    *   com.google.common.primitives.Shorts#toByteArray(short)
    *   com.google.common.primitives.Longs#toByteArray(long)
    *
    * @param bin time bin
    * @param z z value
    * @return
    */
  def toBytes(bin: Short, z: Long): Array[Byte] = {
    val result = Array.ofDim[Byte](10)
    writeShort(bin, result, 0)
    writeLong(z, result, 2)
    result
  }

  /**
    * Creates a byte array with a short and a long, preserving the sort order of the short for negative values
    *
    * @param bin time bin
    * @param z z value
    * @return
    */
  def toOrderedBytes(bin: Short, z: Long): Array[Byte] = {
    val result = Array.ofDim[Byte](10)
    writeOrderedShort(bin, result, 0)
    writeLong(z, result, 2)
    result
  }

  /**
    * Creates a byte array with a short and a long.
    *
    * Code based on the following methods, but avoids allocating extra byte arrays:
    *
    *   com.google.common.primitives.Shorts#toByteArray(short)
    *   com.google.common.primitives.Longs#toByteArray(long)
    *
    * @param bin time bin, already converted to 2 bytes
    * @param z z value
    * @return
    */
  def toBytes(bin: Array[Byte], z: Long): Array[Byte] = {
    val result = Array.ofDim[Byte](10)

    result(0) = bin(0)
    result(1) = bin(1)

    writeLong(z, result, 2)

    result
  }

  /**
    * Converts a UUID into a byte array.
    *
    * Code based on the following method, but avoids allocating extra byte arrays:
    *
    *   com.google.common.primitives.Longs#toByteArray(long)
    *
    * @param msb most significant bits
    * @param lsb least significant bits
    * @return
    */
  def uuidToBytes(msb: Long, lsb: Long): Array[Byte] = {
    val result = Array.ofDim[Byte](16)
    writeLong(msb, result, 0)
    writeLong(lsb, result, 8)
    result
  }

  /**
    * Converts a byte array into a UUID.
    *
    * Code based on the following method:
    *
    *   com.google.common.primitives.Longs#fromByteArray(bytes)
    *
    * @param bytes bytes
    * @return (most significant bits, least significant bits)
    */
  def uuidFromBytes(bytes: Array[Byte], offset: Int = 0): (Long, Long) = {
    val msb = readLong(bytes, offset)
    val lsb = readLong(bytes, offset + 8)
    (msb, lsb)
  }

  /**
    * Creates a byte array that sorts directly after the z-value (as converted into a byte array).
    *
    * Code based on the following methods, but avoids allocating extra byte arrays:
    *
    *   org.apache.accumulo.core.data.Range#followingPrefix(org.apache.hadoop.io.Text)
    *   com.google.common.primitives.Longs#toByteArray(long)
    *
    *
    * @param z z value
    * @return
    */
  def toBytesFollowingPrefix(z: Long): Array[Byte] = incrementInPlace(toBytes(z))

  /**
    * Creates a byte array that sorts directly after the z-value (as converted into a byte array).
    *
    * Code based on the following methods, but avoids allocating extra byte arrays:
    *
    *   org.apache.accumulo.core.data.Range#followingPrefix(org.apache.hadoop.io.Text)
    *   com.google.common.primitives.Shorts#toByteArray(short)
    *   com.google.common.primitives.Longs#toByteArray(long)
    *
    * @param bin epoch bin
    * @param z z value
    * @return
    */
  def toBytesFollowingPrefix(bin: Short, z: Long): Array[Byte] = incrementInPlace(toBytes(bin, z))

  /**
    * Creates a byte array that sorts directly after the z-value (as converted into a byte array).
    *
    * @param bin epoch bin
    * @param z z value
    * @return
    */
  def toOrderedBytesFollowingPrefix(bin: Short, z: Long): Array[Byte] = incrementInPlace(toOrderedBytes(bin, z))

  def toBytesFollowingRow(long: Long): Array[Byte] = {
    val result = Array.ofDim[Byte](9)
    writeLong(long, result)
    result(8) = ZeroByte
    result
  }

  def toBytesFollowingRow(bin: Short, z: Long): Array[Byte] = {
    val result = Array.ofDim[Byte](11)
    writeShort(bin, result, 0)
    writeLong(z, result, 2)
    result(10) = ZeroByte
    result
  }

  def toOrderedBytesFollowingRow(bin: Short, z: Long): Array[Byte] = {
    val result = Array.ofDim[Byte](11)
    writeOrderedShort(bin, result, 0)
    writeLong(z, result, 2)
    result(10) = ZeroByte
    result
  }

  /**
    * Returns a row that sorts just after all rows beginning with a prefix. Copied from Accumulo Range
    *
    * @param prefix to follow
    * @return prefix that immediately follows the given prefix when sorted, or an empty array if no prefix can follow
    *         (i.e., the string is all 0xff bytes)
    */
  def rowFollowingPrefix(prefix: Array[Byte]): Array[Byte] = {
    // find the last byte in the array that is not 0xff
    var changeIndex = prefix.length - 1
    while (changeIndex >= 0 && prefix(changeIndex) == MaxByte) {
      changeIndex -= 1
    }
    if (changeIndex < 0) { Array.empty } else {
      // copy prefix bytes into new array
      val following = Array.ofDim[Byte](changeIndex + 1)
      System.arraycopy(prefix, 0, following, 0, changeIndex + 1)
      // increment the selected byte
      following(changeIndex) = (following(changeIndex) + 1).toByte
      following
    }
  }

  /**
    * Returns a row that immediately follows the row. Useful for inclusive endpoints.
    *
    * @param row row
    * @return
    */
  def rowFollowingRow(row: Array[Byte]): Array[Byte] = {
    val following = Array.ofDim[Byte](row.length + 1)
    System.arraycopy(row, 0, following, 0, row.length)
    following(row.length) = ZeroByte
    following
  }

  /**
    * Returns a row that immediately follows the row. Useful for inclusive endpoints.
    *
    * @param bytes row
    * @return
    */
  def rowFollowingRow(bytes: Array[Byte]*): Array[Byte] = {
    var length = 1
    bytes.foreach(b => length += b.length)
    val result = Array.ofDim[Byte](length)
    var i = 0
    bytes.foreach { b =>
      System.arraycopy(b, 0, result, i, b.length)
      i += b.length
    }
    result(i) = ZeroByte
    result
  }

  /**
    * Concatenate byte arrays
    *
    * @param first first array
    * @param second second array
    * @return
    */
  def concat(first: Array[Byte], second: Array[Byte]): Array[Byte] = {
    val result = Array.ofDim[Byte](first.length + second.length)
    System.arraycopy(first, 0, result, 0, first.length)
    System.arraycopy(second, 0, result, first.length, second.length)
    result
  }

  /**
    * Concatenate byte arrays
    *
    * @param bytes arrays
    * @return
    */
  def concat(bytes: Array[Byte]*): Array[Byte] = {
    var length = 0
    bytes.foreach(b => length += b.length)
    val result = Array.ofDim[Byte](length)
    var i = 0
    bytes.foreach { b =>
      System.arraycopy(b, 0, result, i, b.length)
      i += b.length
    }
    result
  }

  /**
    * Converts an unsigned byte into a hex string
    *
    * @param b unsigned byte
    * @return
    */
  def toHex(b: Byte): String = f"${(b & 0xff) >>> 4}%01x${b & 0x0f}%01x"

  /**
    * Converts an unsigned byte array into a hex string
    *
    * @param bytes unsigned byte array
    * @return
    */
  def toHex(bytes: Array[Byte]): String = toHex(bytes, 0, bytes.length)

  /**
    * Converts an unsigned byte array into a hex string
    *
    * @param bytes unsigned byte array
    * @return
    */
  def toHex(bytes: Array[Byte], offset: Int, length: Int): String = {
    val sb = new StringBuilder(length * 2)
    var i = 0
    while (i < length) {
      sb.append(toHex(bytes(i + offset)))
      i += 1
    }
    sb.toString
  }

  /**
    * Increment the last byte in the array, if it's not equal to MaxByte. Otherwise,
    * walk backwards until we find a byte we can increment, and create a new sub-array
    *
    * @param bytes bytes
    * @return
    */
  private def incrementInPlace(bytes: Array[Byte]): Array[Byte] = {
    var i = bytes.length - 1
    if (bytes(i) != MaxByte) {
      // normal case - we can just update the original byte array
      bytes(i) = (bytes(i) + 1).toByte
      bytes
    } else {
      // walk backwards to find the first byte we can increment, then take the sub-array to that point
      do { i -= 1 } while (i >= 0 && bytes(i) == MaxByte)

      if (i == -1) { Array.empty } else {
        val result = Array.ofDim[Byte](i + 1)
        System.arraycopy(bytes, 0, result, 0, result.length)
        result(i) = (result(i) + 1).toByte
        result
      }
    }
  }
}
