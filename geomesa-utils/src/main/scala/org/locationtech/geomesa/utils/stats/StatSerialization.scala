/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.nio.ByteBuffer
import java.util.Date

import com.google.common.primitives.Bytes

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Stats are serialized as a byte array where the first byte indicates which type of stat is present.
 * The next four bits contain the size of the serialized information.
 * A SeqStat is serialized the same way, with each individual stat immediately following the previous in the byte array.
 */
object StatSerialization {
  // bytes indicating the type of stat
  val MINMAX_BYTE: Byte     = '0'
  val ISC_BYTE: Byte        = '1'
  val EH_BYTE: Byte         = '2'
  val RH_BYTE: Byte         = '3'

  /**
   * Fully serializes a stat by formatting the byte array with the "kind" byte
   *
   * @param kind byte indicating the type of stat
   * @param bytes serialized stat
   * @return fully serialized stat
   */
  private def serializeStat(kind: Byte, bytes: Array[Byte]): Array[Byte] = {
    val size = ByteBuffer.allocate(4).putInt(bytes.length).array
    Bytes.concat(Array(kind), size, bytes)
  }

  protected [stats] def packMinMax(mm: MinMax[_]): Array[Byte] = {
    serializeStat(MINMAX_BYTE, s"${mm.attrIndex};${mm.attrType};${mm.min};${mm.max}".getBytes)
  }

  protected [stats] def unpackMinMax(bytes: Array[Byte]): MinMax[_] = {
    val split = new String(bytes).split(";")
    require(split.size == 4)

    val attrIndex = split(0).toInt
    val attrTypeString = split(1)
    val min = split(2)
    val max = split(3)

    val attrType = Class.forName(attrTypeString)
    attrType match {
      case _ if attrType == classOf[Date] =>
        new MinMax[Date](attrIndex, attrTypeString,
          StatHelpers.javaDateFormat.parse(min), StatHelpers.javaDateFormat.parse(max))
      case _ if attrType == classOf[java.lang.Integer] =>
        new MinMax[java.lang.Integer](attrIndex, attrTypeString, min.toInt, max.toInt)
      case _ if attrType == classOf[java.lang.Long] =>
        new MinMax[java.lang.Long](attrIndex, attrTypeString, min.toLong, max.toLong)
      case _ if attrType == classOf[java.lang.Float] =>
        new MinMax[java.lang.Float](attrIndex, attrTypeString, min.toFloat, max.toFloat)
      case _ if attrType == classOf[java.lang.Double] =>
        new MinMax[java.lang.Double](attrIndex, attrTypeString, min.toDouble, max.toDouble)
    }
  }

  protected [stats] def packISC(isc: IteratorStackCounter): Array[Byte] = {
    serializeStat(ISC_BYTE, s"${isc.count}".getBytes)
  }

  protected [stats] def unpackIteratorStackCounter(bytes: Array[Byte]): IteratorStackCounter = {
    val stat = new IteratorStackCounter()
    stat.count = java.lang.Long.parseLong(new String(bytes))
    stat
  }

  protected [stats] def packEnumeratedHistogram(eh: EnumeratedHistogram[_]): Array[Byte] = {
    val sb = new StringBuilder(s"${eh.attrIndex};${eh.attrType};")

    val keyValues = eh.frequencyMap.map { case (key, count) => s"${key.toString}->$count" }.mkString(",")
    sb.append(keyValues)

    serializeStat(EH_BYTE, sb.toString().getBytes)
  }

  protected [stats] def unpackEnumeratedHistogram(bytes: Array[Byte]): EnumeratedHistogram[_] = {
    val split = new String(bytes).split(";")
    require(split.size == 3)

    val attrIndex = split(0).toInt
    val attrTypeString = split(1)
    val keyValues = split(2).split(",")

    val attrType = Class.forName(attrTypeString)
    attrType match {
      case _ if attrType == classOf[Date] =>
        val eh = new EnumeratedHistogram[Date](attrIndex, attrTypeString)
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            eh.frequencyMap.put(StatHelpers.javaDateFormat.parse(splitKeyValuePair(0)), splitKeyValuePair(1).toLong)
        }
        eh
      case _ if attrType == classOf[Integer] =>
        val eh = new EnumeratedHistogram[Integer](attrIndex, attrTypeString)
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            eh.frequencyMap.put(splitKeyValuePair(0).toInt, splitKeyValuePair(1).toLong)
        }
        eh
      case _ if attrType == classOf[java.lang.Long] =>
        val eh = new EnumeratedHistogram[java.lang.Long](attrIndex, attrTypeString)
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            eh.frequencyMap.put(splitKeyValuePair(0).toLong, splitKeyValuePair(1).toLong)
        }
        eh
      case _ if attrType == classOf[java.lang.Float] =>
        val eh = new EnumeratedHistogram[java.lang.Float](attrIndex, attrTypeString)
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            eh.frequencyMap.put(splitKeyValuePair(0).toFloat, splitKeyValuePair(1).toLong)
        }
        eh
      case _ if attrType == classOf[java.lang.Double] =>
        val eh = new EnumeratedHistogram[java.lang.Double](attrIndex, attrTypeString)
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            eh.frequencyMap.put(splitKeyValuePair(0).toDouble, splitKeyValuePair(1).toLong)
        }
        eh
    }
  }

  protected [stats] def packRangeHistogram(rh: RangeHistogram[_]): Array[Byte] = {
    val sb = new StringBuilder(s"${rh.attrIndex};${rh.attrType};${rh.numBins};${rh.lowerEndpoint};${rh.upperEndpoint};")

    val keyValues = rh.histogram.map { case (key, count) => s"${key.toString}->$count" }.mkString(",")
    sb.append(keyValues)

    serializeStat(RH_BYTE, sb.toString().getBytes)
  }

  protected [stats] def unpackRangeHistogram(bytes: Array[Byte]): RangeHistogram[_] = {
    val split = new String(bytes).split(";")
    require(split.size == 6)

    val attrIndex = split(0).toInt
    val attrTypeString = split(1)
    val numBins = split(2)
    val lowerEndpoint = split(3)
    val upperEndpoint = split(4)
    val keyValues = split(5).split(",")

    val attrType = Class.forName(attrTypeString)
    attrType match {
      case _ if attrType == classOf[Date] =>
        val rh = new RangeHistogram[Date](attrIndex, attrTypeString, numBins.toInt,
          StatHelpers.javaDateFormat.parse(lowerEndpoint), StatHelpers.javaDateFormat.parse(upperEndpoint))
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            rh.histogram.put(StatHelpers.javaDateFormat.parse(splitKeyValuePair(0)), splitKeyValuePair(1).toLong)
        }
        rh
      case _ if attrType == classOf[Integer] =>
        val rh = new RangeHistogram[Integer](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toInt, upperEndpoint.toInt)
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            rh.histogram.put(splitKeyValuePair(0).toInt, splitKeyValuePair(1).toLong)
        }
        rh
      case _ if attrType == classOf[java.lang.Long] =>
        val rh = new RangeHistogram[java.lang.Long](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toLong, upperEndpoint.toLong)
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            rh.histogram.put(splitKeyValuePair(0).toLong, splitKeyValuePair(1).toLong)
        }
        rh
      case _ if attrType == classOf[java.lang.Float] =>
        val rh = new RangeHistogram[java.lang.Float](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toFloat, upperEndpoint.toFloat)
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            rh.histogram.put(splitKeyValuePair(0).toFloat, splitKeyValuePair(1).toLong)
        }
        rh
      case _ if attrType == classOf[java.lang.Double] =>
        val rh = new RangeHistogram[java.lang.Double](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toDouble, upperEndpoint.toDouble)
        keyValues.foreach {
          case (keyValuePair) =>
            val splitKeyValuePair = keyValuePair.split("->")
            rh.histogram.put(splitKeyValuePair(0).toDouble, splitKeyValuePair(1).toLong)
        }
        rh
    }
  }

  /**
   * Uses individual stat pack methods to serialize the stat
   *
   * @param stat the given stat to serialize
   * @return serialized stat
   */
  def pack(stat: Stat): Array[Byte] = {
    stat match {
      case mm: MinMax[_]                => packMinMax(mm)
      case isc: IteratorStackCounter    => packISC(isc)
      case eh: EnumeratedHistogram[_]   => packEnumeratedHistogram(eh)
      case rh: RangeHistogram[_]        => packRangeHistogram(rh)
      case seq: SeqStat                 => Bytes.concat(seq.stats.map(pack) : _*)
    }
  }

  /**
   * Deserializes the stat
   *
   * @param bytes the serialized stat
   * @return deserialized stat
   */
  def unpack(bytes: Array[Byte]): Stat = {
    val returnStats: ArrayBuffer[Stat] = new mutable.ArrayBuffer[Stat]()
    val bb = ByteBuffer.wrap(bytes)

    var bytePointer = 0
    while (bytePointer < bytes.length - 1) {
      val statType = bytes(bytePointer)
      val statSize = bb.getInt(bytePointer + 1)
      val statBytes = bytes.slice(bytePointer + 5, bytePointer + 5 + statSize)

      statType match {
        case MINMAX_BYTE =>
          val stat = unpackMinMax(statBytes)
          returnStats += stat
        case ISC_BYTE =>
          val stat = unpackIteratorStackCounter(statBytes)
          returnStats += stat
        case EH_BYTE =>
          val stat = unpackEnumeratedHistogram(statBytes)
          returnStats += stat
        case RH_BYTE =>
          val stat = unpackRangeHistogram(statBytes)
          returnStats += stat
      }

      bytePointer += statSize + 5
    }

    returnStats.size match {
      case 1 => returnStats.head
      case _ => new SeqStat(returnStats.toSeq)
    }
  }
}
