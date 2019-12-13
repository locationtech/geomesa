/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/**
  * Portions derived from google protocol buffers, which are:
  *
  * Copyright 2008 Google Inc.  All rights reserved.
  *
  * Redistribution and use in source and binary forms, with or without
  * modification, are permitted provided that the following conditions are
  * met:
  *
  * * Redistributions of source code must retain the above copyright
  * notice, this list of conditions and the following disclaimer.
  * * Redistributions in binary form must reproduce the above
  * copyright notice, this list of conditions and the following disclaimer
  * in the documentation and/or other materials provided with the
  * distribution.
  * * Neither the name of Google Inc. nor the names of its
  * contributors may be used to endorse or promote products derived from
  * this software without specific prior written permission.
  *
  * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  *
  */

package org.locationtech.geomesa.features.serialization

// noinspection LanguageFeature
trait VarIntEncoding[T <: NumericWriter, V <: NumericReader] {

  /**
    * Writes a variable length signed int. The int will first be zig-zag encoded to prevent small
    * negative values from taking up the max number of bytes
    *
    * @param writer writer
    * @param value int
    */
  def writeVarInt(writer: T, value: Int): Unit = writeUnsignedVarInt(writer, VarIntEncoding.zigzagEncode(value))

  /**
    * Writes a variable length unsigned int. Small values will be encoded with fewer bytes
    *
    * @param writer writer
    * @param value int
    */
  def writeUnsignedVarInt(writer: T, value: Int): Unit = {
    var remaining = value
    while ((remaining & ~0x7F) != 0) {
      writer.writeByte(((remaining & 0x7F) | 0x80).toByte)
      remaining = remaining >>> 7
    }
    writer.writeByte(remaining.toByte)
  }

  /**
    * Reads a signed int encoded with `writeVarInt`
    *
    * @param reader reader
    * @return
    */
  def readVarInt(reader: V): Int = VarIntEncoding.zigzagDecode(readUnsignedVarInt(reader))

  /**
    * Reads an unsigned int encoded with `writeUnsignedVarInt`
    *
    * @param reader reader
    * @return
    */
  def readUnsignedVarInt(reader: V): Int = {
    var byte = reader.readByte()
    if ((byte & 0x80) == 0) {
      byte
    } else {
      var result = byte & 0x7F
      var offset = 7
      while (offset < 32) {
        byte = reader.readByte()
        result = result | ((byte & 0x7f) << offset)
        if ((byte & 0x80) == 0) {
          return result
        }
        offset += 7
      }
      throw new IllegalArgumentException("Did not find last byte of encoded VarInt")
    }
  }

  /**
    * Skip over a variable length int, either signed or unsigned
    *
    * @param reader reader
    */
  def skipVarInt(reader: V): Unit = {
    var byte = reader.readByte()
    if ((byte & 0x80) != 0) {
      byte = reader.readByte()
      var count = 2
      while ((byte & 0x80) != 0) {
        if (count == 5) {
          throw new IllegalArgumentException("Did not find last byte of encoded VarInt")
        }
        byte = reader.readByte()
        count += 1
      }
    }
  }
}

object VarIntEncoding {

  /**
    * Takes a signed int and zig-zag encodes it to an unsigned int
    *
    * See https://github.com/TWKB/Specification/blob/master/twkb.md#zigzag-encode
    *
    * @param n signed int
    * @return unsigned int, with small absolute values encoded as small numbers
    */
  def zigzagEncode(n: Int): Int = (n << 1) ^ (n >> 31)

  /**
    * Takes a zig-zag encoded unsigned int and restores the original signed int
    *
    * @param n unsigned, zig-zag encoded int
    * @return signed int
    */
  def zigzagDecode(n: Int): Int = (n >>> 1) ^ -(n & 1)
}
