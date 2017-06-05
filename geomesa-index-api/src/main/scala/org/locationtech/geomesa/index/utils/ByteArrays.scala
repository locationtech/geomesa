/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

object ByteArrays {

  val ZeroByte: Byte = 0x00.toByte
  val MaxByte: Byte =  0xff.toByte

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

    result(2) = ((z >> 56) & 0xff).asInstanceOf[Byte]
    result(3) = ((z >> 48) & 0xff).asInstanceOf[Byte]
    result(4) = ((z >> 40) & 0xff).asInstanceOf[Byte]
    result(5) = ((z >> 32) & 0xff).asInstanceOf[Byte]
    result(6) = ((z >> 24) & 0xff).asInstanceOf[Byte]
    result(7) = ((z >> 16) & 0xff).asInstanceOf[Byte]
    result(8) = ((z >> 8)  & 0xff).asInstanceOf[Byte]
    result(9) = (z & 0xff        ).asInstanceOf[Byte]

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

    result(2) = ((z >> 56) & 0xff).asInstanceOf[Byte]
    result(3) = ((z >> 48) & 0xff).asInstanceOf[Byte]
    result(4) = ((z >> 40) & 0xff).asInstanceOf[Byte]
    result(5) = ((z >> 32) & 0xff).asInstanceOf[Byte]
    result(6) = ((z >> 24) & 0xff).asInstanceOf[Byte]
    result(7) = ((z >> 16) & 0xff).asInstanceOf[Byte]
    result(8) = ((z >> 8)  & 0xff).asInstanceOf[Byte]
    result(9) = (z & 0xff        ).asInstanceOf[Byte]

    result
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
    val b7 = (z & 0xff        ).asInstanceOf[Byte]

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
    val b9 = (z & 0xff        ).asInstanceOf[Byte]

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
