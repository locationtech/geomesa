/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

package object serialization {

  val NULL_BYTE    : Byte = 0
  val NOT_NULL_BYTE: Byte = 1

  type PrimitiveWriter = AnyRef {
    def writeInt(value: Int): Unit
    def writeLong(value: Long): Unit
    def writeFloat(value: Float): Unit
    def writeDouble(value: Double): Unit
    def writeBoolean(value: Boolean): Unit
    def writeString(value: String): Unit
  }

  type PrimitiveReader = AnyRef {
    def readInt(): Int
    def readLong(): Long
    def readFloat(): Float
    def readDouble(): Double
    def readBoolean(): Boolean
    def readString(): String
  }

  type NumericWriter = AnyRef {
    def writeInt(value: Int, optimizePositive: Boolean): Int
    def writeDouble(value: Double): Unit
    def writeByte(value: Byte): Unit
  }

  type NumericReader = AnyRef {
    def readInt(optimizePositive: Boolean): Int
    def readDouble(): Double
    def readByte(): Byte
  }
}
