/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.nio.ByteBuffer

import com.google.common.primitives.Bytes

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object StatSerialization {

  val MINMAX_BYTE: Byte = '0'
  val ISC_BYTE: Byte    = '1'

  private def wrap(kind: Byte, bytes: Array[Byte]): Array[Byte] = {
    val size = ByteBuffer.allocate(4).putInt(bytes.length).array
    Bytes.concat(Array(kind), size, bytes)
  }

  protected [stats] def packMinMax[T <: Comparable[T]](mm: MinMax[T]): Array[Byte] = {
    wrap(MINMAX_BYTE, s"${mm.attribute};${mm.min};${mm.max}".getBytes())
  }

  def packISC(isc: IteratorStackCounter): Array[Byte] = {
    wrap(ISC_BYTE, s"${isc.count}".getBytes())
  }

  protected [stats] def unpackMinMax(bytes: Array[Byte]): MinMax[java.lang.Long] = {
    val split = new String(bytes).split(";")

    require(split.size == 3)

    val stat = new MinMax[java.lang.Long](split(0))
    stat.min = java.lang.Long.parseLong(split(1))
    stat.max = java.lang.Long.parseLong(split(2))
    stat
  }

  def unpackIteratorStackCounter(bytes: Array[Byte]): IteratorStackCounter = {
    val stat = new IteratorStackCounter
    stat.count = java.lang.Long.parseLong(new String(bytes))
    stat
  }

  def pack(stat: Stat): Array[Byte] = {
    // Uses individual pack m
    stat match {
      case mm: MinMax[_]             => packMinMax(mm)
      case isc: IteratorStackCounter => packISC(isc)
      case seq: SeqStat              => Bytes.concat(seq.stats.map(pack) : _*)
    }
  }

  def unpack(bytes: Array[Byte]): Stat = {

    val returnStats: ArrayBuffer[Stat] = new mutable.ArrayBuffer[Stat]()
    var totalSize = bytes.length
    val bb = ByteBuffer.wrap(bytes)

    var pointer = 0

    // Loop begins
    while (pointer < totalSize - 1) {

      val kind = bytes(pointer)
      val size = bb.getInt(pointer + 1)

      kind match {
        case MINMAX_BYTE =>
          val stat = unpackMinMax(bytes.slice(pointer + 5, pointer + 5 + size))
          returnStats += stat
        case ISC_BYTE =>
          val stat = unpackIteratorStackCounter(bytes.slice(pointer + 5, pointer + 5 + size))
          returnStats += stat
      }
      pointer += size + 5
      // Loop Ends
    }

    returnStats.size match {
      case 1 => returnStats.head
      case _ => new SeqStat(returnStats.toSeq)
    }
  }
}
