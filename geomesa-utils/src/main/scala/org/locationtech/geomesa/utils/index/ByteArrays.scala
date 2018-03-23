/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

object ByteArrays {

  val ZeroByte: Byte = 0x00.toByte
  val MaxByte: Byte =  0xff.toByte

  /**
    * Writes the long as 8 bytes in the provided array, starting at offset
    *
    * @param long long to write
    * @param bytes byte array to write to, must have length at least `offset` + 8
    * @param offset offset to start writing
    */
  def writeToByteArray(long: Long, bytes: Array[Byte], offset: Int = 0): Unit = {
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
    * Reads 8 bytes from the provided array as a long, starting at offset
    *
    * @param bytes array to read from
    * @param offset offset to start reading
    * @return
    */
  def readFromByteArray(bytes: Array[Byte], offset: Int = 0): Long = {
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

    result(0) = (bin >> 8).asInstanceOf[Byte]
    result(1) = bin.asInstanceOf[Byte]

    writeToByteArray(z, result, 2)

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

    writeToByteArray(z, result, 2)

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
    writeToByteArray(msb, result, 0)
    writeToByteArray(lsb, result, 8)
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
    val msb = readFromByteArray(bytes, offset)
    val lsb = readFromByteArray(bytes, offset + 8)
    (msb, lsb)
  }

  /**
    * Creates a byte array that sorts directly after the z-value (as converted into a byte array).
    *
    * Code based on the following methods, but avoids allocating extra byte arrays:
    *
    *   org.apache.accumulo.core.data.Range#followingPrefix(org.apache.hadoop.io.Text)
    *   com.google.common.primitives.Shorts#toByteArray(short)
    *   com.google.common.primitives.Longs#toByteArray(long)
    *
    *
    * @param z z value
    * @return
    */
  def toBytesFollowingPrefix(z: Long): Array[Byte] = {
    val b0 = ((z >> 56) & 0xff).asInstanceOf[Byte]
    val b1 = ((z >> 48) & 0xff).asInstanceOf[Byte]
    val b2 = ((z >> 40) & 0xff).asInstanceOf[Byte]
    val b3 = ((z >> 32) & 0xff).asInstanceOf[Byte]
    val b4 = ((z >> 24) & 0xff).asInstanceOf[Byte]
    val b5 = ((z >> 16) & 0xff).asInstanceOf[Byte]
    val b6 = ((z >> 8)  & 0xff).asInstanceOf[Byte]
    val b7 = ( z        & 0xff).asInstanceOf[Byte]

    // find the last byte in the array that is not 0xff and increment it
    if (b7 != MaxByte) {
      Array(b0, b1, b2, b3, b4, b5, b6, (b7 + 1).toByte)
    } else if (b6 != MaxByte) {
      Array(b0, b1, b2, b3, b4, b5, (b6 + 1).toByte)
    } else if (b5 != MaxByte) {
      Array(b0, b1, b2, b3, b4, (b5 + 1).toByte)
    } else if (b4 != MaxByte) {
      Array(b0, b1, b2, b3, (b4 + 1).toByte)
    } else if (b3 != MaxByte) {
      Array(b0, b1, b2, (b3 + 1).toByte)
    } else if (b2 != MaxByte) {
      Array(b0, b1, (b2 + 1).toByte)
    } else if (b1 != MaxByte) {
      Array(b0, (b1 + 1).toByte)
    } else if (b0 != MaxByte) {
      Array((b0 + 1).toByte)
    } else {
      Array.empty
    }
  }

  /**
    * Creates a byte array that sorts directly after the z-value (as converted into a byte array).
    *
    * Code based on the following methods, but avoids allocating extra byte arrays:
    *
    *   org.apache.accumulo.core.data.Range#followingPrefix(org.apache.hadoop.io.Text)
    *   com.google.common.primitives.Shorts#toByteArray(short)
    *   com.google.common.primitives.Longs#toByteArray(long)
    *
    * @param z z value
    * @return
    */
  def toBytesFollowingPrefix(bin: Array[Byte], z: Long): Array[Byte] = {
    val b0 = bin(0)
    val b1 = bin(1)

    val b2 = ((z >> 56) & 0xff).asInstanceOf[Byte]
    val b3 = ((z >> 48) & 0xff).asInstanceOf[Byte]
    val b4 = ((z >> 40) & 0xff).asInstanceOf[Byte]
    val b5 = ((z >> 32) & 0xff).asInstanceOf[Byte]
    val b6 = ((z >> 24) & 0xff).asInstanceOf[Byte]
    val b7 = ((z >> 16) & 0xff).asInstanceOf[Byte]
    val b8 = ((z >> 8)  & 0xff).asInstanceOf[Byte]
    val b9 = ( z        & 0xff).asInstanceOf[Byte]

    // find the last byte in the array that is not 0xff and increment it
    if (b9 != MaxByte) {
      Array(b0, b1, b2, b3, b4, b5, b6, b7, b8, (b9 + 1).toByte)
    } else if (b8 != MaxByte) {
      Array(b0, b1, b2, b3, b4, b5, b6, b7, (b8 + 1).toByte)
    } else if (b7 != MaxByte) {
      Array(b0, b1, b2, b3, b4, b5, b6, (b7 + 1).toByte)
    } else if (b6 != MaxByte) {
      Array(b0, b1, b2, b3, b4, b5, (b6 + 1).toByte)
    } else if (b5 != MaxByte) {
      Array(b0, b1, b2, b3, b4, (b5 + 1).toByte)
    } else if (b4 != MaxByte) {
      Array(b0, b1, b2, b3, (b4 + 1).toByte)
    } else if (b3 != MaxByte) {
      Array(b0, b1, b2, (b3 + 1).toByte)
    } else if (b2 != MaxByte) {
      Array(b0, b1, (b2 + 1).toByte)
    } else if (b1 != MaxByte) {
      Array(b0, (b1 + 1).toByte)
    } else if (b0 != MaxByte) {
      Array((b0 + 1).toByte)
    } else {
      Array.empty
    }
  }

  def concat(first: Array[Byte], second: Array[Byte]): Array[Byte] = {
    val result = Array.ofDim[Byte](first.length + second.length)
    System.arraycopy(first, 0, result, 0, first.length)
    System.arraycopy(second, 0, result, first.length, second.length)
    result
  }

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
  def toHex(bytes: Array[Byte]): String = bytes.map(toHex).mkString
}
