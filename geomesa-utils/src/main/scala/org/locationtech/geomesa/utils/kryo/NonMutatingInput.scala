/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/*
 * Portions derived from kryo (https://github.com/EsotericSoftware/kryo), which are:
 *
 * Copyright (c) 2008-2018, Nathan Sweet
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided with the distribution.
 * - Neither the name of Esoteric Software nor the names of its contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.locationtech.geomesa.utils.kryo

import com.esotericsoftware.kryo.io.Input

/**
 * Kryo input that doesn't mutate the underlying buffer when reading
 */
class NonMutatingInput extends Input {

  def this(buffer: Array[Byte]) = {
    this()
    setBuffer(buffer)
  }

  override def readString(): String = {
    if (position == limit) {
      require(1)
    }
    if ((buffer(position) & 0x80) == 0) {
      readAsciiString()
    } else {
      // null, empty or utf8
      var charCount = readVarIntLength()
      if (charCount == 0) {
        null
      } else if (charCount == 1) {
        ""
      } else {
        charCount -= 1
        readUtf8Chars(charCount)
        new String(chars, 0, charCount)
      }
    }
  }

  override def readStringBuilder(): java.lang.StringBuilder = {
    if (position == limit) {
      require(1)
    }
    if ((buffer(position) & 0x80) == 0) {
      new java.lang.StringBuilder(readAsciiString())
    } else {
      // null, empty or utf8
      var charCount = readVarIntLength()
      if (charCount == 0) {
        null
      } else if (charCount == 1) {
        new java.lang.StringBuilder(0)
      } else {
        charCount -= 1
        readUtf8Chars(charCount)
        val builder = new java.lang.StringBuilder(charCount)
        builder.append(chars, 0, charCount)
        builder
      }
    }
  }

  private def readAsciiString(): String = {
    val chars = this.chars
    val buffer = this.buffer
    var charCount = 0
    var p = position
    val n = Math.min(chars.length, limit - p)
    while (charCount < n) {
      val b = buffer(p)
      if ((b & 0x80) == 0x80) {
        position = p + 1
        chars(charCount) = (b & 0x7F).toChar
        return new String(chars, 0, charCount + 1)
      }
      chars(charCount) = b.toChar
      charCount += 1
      p += 1
    }
    position = p
    readAsciiString_slow(charCount)
  }

  private def readAsciiString_slow(count: Int): String = {
    var chars = this.chars
    val buffer = this.buffer
    var charCount = count
    var p = position
    while (true) {
      if (position == limit) {
        require(1)
      }
      val b = buffer(p)
      if (charCount == chars.length) {
        val newChars = new Array[Char](charCount * 2)
        System.arraycopy(chars, 0, newChars, 0, charCount)
        chars = newChars
        this.chars = chars
      }
      if ((b & 0x80) == 0x80) {
        position = p + 1
        chars(charCount) = (b & 0x7F).toChar
        return new String(chars, 0, charCount + 1)
      }
      chars(charCount) = b.toChar
      charCount += 1
      p += 1
    }
    throw new NotImplementedError() // can't ever reach here
  }

  private def readUtf8Chars(charCount: Int): Unit = {
    if (this.chars.length < charCount) {
      this.chars = Array.ofDim[Char](charCount)
    }
    val buffer = this.buffer
    val chars = this.chars
    // try to read 7 bit ASCII chars
    var count = Math.min(require(1), charCount)
    var charIndex = 0
    var p = position
    var b = 0
    while (charIndex < count) {
      b = buffer(p)
      if (b < 0) {
        count = 0 // break out of the loop
      } else {
        chars(charIndex) = b.toChar
        charIndex += 1
        p += 1
      }
    }
    position = p
    // if buffer didn't hold all chars or any were not ASCII, use slow path for remainder.
    if (charIndex < charCount) {
      readUtf8Chars_slow(charCount, charIndex)
    }
  }

  private def readUtf8Chars_slow(charCount: Int, index: Int): Unit = {
    val chars = this.chars
    val buffer = this.buffer
    var charIndex = index
    while (charIndex < charCount) {
      if (position == limit) {
        require(1)
      }
      val b = buffer(position) & 0xFF
      position += 1
      val i = b >> 4
      if (i >= 0 && i < 8) {
        chars(charIndex) = b.toChar
      } else if (i == 12 || i == 13) {
        if (position == limit) {
          require(1)
        }
        chars(charIndex) = ((b & 0x1F) << 6 | buffer(position) & 0x3F).toChar
        position += 1
      } else if (i == 14) {
        require(2)
        chars(charIndex) = ((b & 0x0F) << 12 | (buffer(position) & 0x3F) << 6 | buffer(position + 1) & 0x3F).toChar
        position += 2
      }
      charIndex += 1
    }
  }

  private def readVarIntLength(): Int = {
    if (require(1) < 5) {
      readVarIntLength_slow()
    } else {
      var b = buffer(position)
      position += 1
      var result = b & 0x3F // mask first 6 bits
      if ((b & 0x40) != 0) { // bit 7 means another byte, bit 8 means UTF8
        b = buffer(position)
        position += 1
        result |= (b & 0x7F) << 6
        if ((b & 0x80) != 0) {
          b = buffer(position)
          position += 1
          result |= (b & 0x7F) << 13
          if ((b & 0x80) != 0) {
            b = buffer(position)
            position += 1
            result |= (b & 0x7F) << 20
            if ((b & 0x80) != 0) {
              b = buffer(position)
              position += 1
              result |= (b & 0x7F) << 27
            }
          }
        }
      }
      result
    }
  }

  private def readVarIntLength_slow(): Int = {
    // the buffer is guaranteed to have at least 1 byte.
    var b = buffer(position)
    position += 1
    var result = b & 0x3F // mask first 6 bits
    if ((b & 0x40) != 0) {
      if (position == limit) {
        require(1)
      }
      b = buffer(position)
      position += 1
      result |= (b & 0x7F) << 6
      if ((b & 0x80) != 0) {
        if (position == limit) {
          require(1)
        }
        b = buffer(position)
        position += 1
        result |= (b & 0x7F) << 13
        if ((b & 0x80) != 0) {
          if (position == limit) {
            require(1)
          }
          b = buffer(position)
          position += 1
          result |= (b & 0x7F) << 20
          if ((b & 0x80) != 0) {
            if (position == limit) {
              require(1)
            }
            b = buffer(position)
            position += 1
            result |= (b & 0x7F) << 27
          }
        }
      }
    }
    result
  }
}
