/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import java.util.{Date, Locale}

import com.google.common.primitives.{Bytes, Longs, Shorts}
import org.geotools.util.Converters
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.index.{AttributeIndex, IdIndex}
import org.opengis.feature.simple.SimpleFeatureType

package object splitter {

  class DefaultSplitter extends TableSplitter {
    override def getSplits(index: String,
                           sft: SimpleFeatureType,
                           options: java.util.Map[String, String]): Array[Array[Byte]] = {
      val typ = Option(options.get(s"$index.type")).map(_.toLowerCase(Locale.US))
      if (typ.contains(Z3Index.Name) || (typ.isEmpty && index == Z3Index.Name)) {
        z3Splits(index, sft, options)
      } else if (typ.contains(Z2Index.Name) || (typ.isEmpty && index == Z2Index.Name)) {
        z2Splits(index, sft, options)
      } else if (typ.contains(AttributeIndex.Name) || (typ.isEmpty && index == AttributeIndex.Name)) {
        attributeSplits(index, sft, options)
      } else if (typ.contains(IdIndex.Name) || (typ.isEmpty && index == IdIndex.Name)) {
        idSplits(index, sft, options)
      } else if (typ.contains("none") || typ.isEmpty) {
        Array(Array.empty)
      } else {
        throw new IllegalArgumentException(s"Unhandled split type '${typ.get}'")
      }
    }
  }

  private def z3Splits(index: String,
                       sft: SimpleFeatureType,
                       options: java.util.Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val min = options.get(s"$index.min")
    val max = options.get(s"$index.max")
    val minDate = Converters.convert(min, classOf[Date])
    var maxDate = Option(Converters.convert(max, classOf[Date]))
    if (min == null) {
      Array(Array.empty)
    } else if (minDate == null || (max != null && maxDate.isEmpty)) {
      throw new IllegalArgumentException(s"Could not convert dates '$min/$max' for splits")
    } else {
      val toBin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
      val minBin = toBin(minDate.getTime).bin
      val maxBin = toBin(maxDate.map(_.getTime).getOrElse(System.currentTimeMillis())).bin
      val times = Array.tabulate(maxBin - minBin + 1)(i => Shorts.toByteArray((minBin + i).toShort))
      Option(options.get(s"$index.bits")).map(_.toInt) match {
        case None => times
        case Some(bits) =>
          // note: first bit in z value is not used, and is always 0
          val zBytes = bitSplit(bits, 1)
          times.flatMap(time => zBytes.map(Bytes.concat(time, _)))
      }
    }
  }

  private def z2Splits(index: String,
                       sft: SimpleFeatureType,
                       options: java.util.Map[String, String]): Array[Array[Byte]] = {
    // note: first 2 bits in z value are not used, and are always 0
    Option(options.get(s"$index.bits")).map(b => bitSplit(b.toInt, 2)).getOrElse(Array(Array.empty))
  }

  private def attributeSplits(index: String,
                              sft: SimpleFeatureType,
                              options: java.util.Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._

    import scala.collection.JavaConversions._

    sft.getAttributeDescriptors.collect {
      case ad if ad.isIndexed => patternSplit(s"$index.${ad.getLocalName}", options)
    }.reduce(_ ++ _)
  }

  private def idSplits(index: String,
                       sft: SimpleFeatureType,
                       options: java.util.Map[String, String]): Array[Array[Byte]] = {
    Option(patternSplit(index, options)).filter(s => s.length > 0 && s(0).length > 0).getOrElse(hexSplits())
  }

  private def bitSplit(bits: Int, maskedBits: Int): Array[Array[Byte]] = {
    require(bits > 0 && bits < 64, "Bit split must be between 1 and 63")

    // recursive function to create all bit permutations
    def add(result: Seq[Seq[Int]], bits: Seq[Int], remaining: Int): Seq[Seq[Int]] = {
      if (remaining == 0) { Seq(bits) } else {
        val left = add(result, bits :+ 0, remaining - 1)
        val right = add(result, bits :+ 1, remaining - 1)
        result ++ left ++ right
      }
    }
    // note: first bit in z value is not used, and is always 0
    def toBytes(bits: Seq[Int]): Array[Byte] = {
      val binaryString = (Seq.fill(maskedBits)(0) ++ bits).padTo(64, 0).mkString("")
      Longs.toByteArray(java.lang.Long.parseLong(binaryString, 2))
    }

    add(Seq.empty, Seq.empty, bits).map(toBytes).toArray
  }

  private def patternSplit(name: String, options: java.util.Map[String, String]): Array[Array[Byte]] = {
    // TODO more robust parsing
    val patterns = options.get(s"$name.pattern")
    if (patterns == null) { Array(Array.empty) } else {
      val splits = patterns.drop(1).dropRight(1).split("\\]\\[").map {
        case "a-z" => lowercaseSplits()
        case "A-Z" => uppercaseSplits()
        case "0-9" => digitSplits()
        case "hex" => hexSplits()
        case "alphanumeric" => alphanumericSplits()
        case pattern =>
          val Array(start, end) = pattern.split("-")
          rangeSplits(start.head, end.head)
      }
      splits.reduceLeft((left, right) => for (a <- left; b <- right) yield Bytes.concat(a, b) )
    }
  }

  // note: we don't include 0 to avoid an empty initial tablet
  private def hexSplits(): Array[Array[Byte]] = "123456789abcdefABCDEF".map(b => Array(b.toByte)).toArray

  // TODO note: we don't include 'A' to avoid an empty initial tablet
  private def uppercaseSplits(from: Char = 'A', to: Char = 'Z'): Array[Array[Byte]] = rangeSplits(from, to)

  // TODO note: we don't include 'a' to avoid an empty initial tablet
  private def lowercaseSplits(from: Char = 'a', to: Char = 'z'): Array[Array[Byte]] = rangeSplits(from, to)

  // TODO note: we don't include 0 to avoid an empty initial tablet
  private def digitSplits(from: Char = '0', to: Char = '9'): Array[Array[Byte]] = rangeSplits(from, to)

  // note: we don't include 0 to avoid an empty initial tablet
  private def alphanumericSplits(): Array[Array[Byte]] = digitSplits() ++ lowercaseSplits() ++ uppercaseSplits()

  private def rangeSplits(from: Char, to: Char): Array[Array[Byte]] =
    Array.range(from, to + 1).map(b => Array(b.toByte))
}
