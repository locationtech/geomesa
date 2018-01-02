/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import java.nio.charset.StandardCharsets
import java.util
import java.util.{Date, Locale}

import com.google.common.primitives.{Bytes, Longs, Shorts}
import org.geotools.util.Converters
import org.locationtech.geomesa.curve.BinnedTime
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

package object splitter {

  class DefaultSplitter extends TableSplitter {

    override def getSplits(index: String,
                           sft: SimpleFeatureType,
                           options: java.util.Map[String, String]): Array[Array[Byte]] = {
      // TODO make these constants
      Option(options.get(s"$index.type")).map(_.toLowerCase(Locale.US)) match {
        case Some("z3")            => z3Splits(sft, options)
        case Some("z2")            => z2Splits(sft, options)
        case Some("attribute")     => attributeSplits(sft, options)
        case None if index == "id" => hexSplits() // TODO defaults for other indices?
        case None | Some("none")   => Array(Array.empty)
        case Some(t) => throw new IllegalArgumentException(s"Unhandled split type '$t'")
      }
    }
  }

  private def z3Splits(sft: SimpleFeatureType, options: java.util.Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val min = options.get("min")
    val max = options.get("max")
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
      Option(options.get("bits")).map(_.toInt).filter(_ > 0) match {
        case None => times
        case Some(bits) =>
          require(bits < 32, "Z3 bits must be less than 32")
          def add(result: Seq[Seq[Int]], bits: Seq[Int], remaining: Int): Seq[Seq[Int]] = {
            if (remaining == 0) {
              Seq(bits)
            } else {
              val left = add(result, bits :+ 0, remaining - 1)
              val right = add(result, bits :+ 1, remaining - 1)
              result ++ left ++ right
            }
          }
          // first bit is always 0
          // min value is 0
          // max value is all ones (except first bit)
          val permutations = add(Seq.empty, Seq.empty, bits - 1)
          val bitBytes = permutations.map(p => Longs.toByteArray(java.lang.Long.parseLong("0" + p.mkString("").padTo(63, '0'), 2)))
          times.flatMap(time => bitBytes.map(Bytes.concat(time, _)))
      }
    }
  }

  private def z2Splits(sft: SimpleFeatureType, options: java.util.Map[String, String]): Array[Array[Byte]] = {
    // TODO
    Array(Array.empty)
  }

  private def attributeSplits(sft: SimpleFeatureType, options: java.util.Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._

    import scala.collection.JavaConversions._

    sft.getAttributeDescriptors.filter(_.isIndexed).map(ad => splitsForAttribute(ad, sft, options)).reduce(_ ++ _)
  }

  private def splitsForAttribute(ad: AttributeDescriptor,
                         sft: SimpleFeatureType,
                         options: util.Map[String, String]): Array[Array[Byte]] = {
    // TODO more robust parsing
    val patterns = options.get(s"${ad.getLocalName}.pattern").drop(1).dropRight(1).split("\\]\\[")
    val splits = patterns.map {
      case "a-z" => lowercaseSplits()
      case "A-Z" => uppercaseSplits()
      case "0-9" => digitSplits()
      case "hex" => hexSplits()
      case "alphanumeric" => alphanumericSplits()
      case pattern =>
        val Array(start, end) = pattern.split("-")
        rangeSplits(start.getBytes(StandardCharsets.UTF_8).head, end.getBytes(StandardCharsets.UTF_8).head)
    }
    splits.reduceLeft((left, right) => for (a <- left; b <- right) yield Bytes.concat(a, b))

  }

  // TODO note: we don't include 'A' to avoid an empty initial tablet
  private def uppercaseSplits(from: Char = 'A', to: Char = 'Z'): Array[Array[Byte]] = rangeSplits(from, to)

  // TODO note: we don't include 'a' to avoid an empty initial tablet
  private def lowercaseSplits(from: Char = 'a', to: Char = 'z'): Array[Array[Byte]] = rangeSplits(from, to)

  // TODO note: we don't include 0 to avoid an empty initial tablet
  private def digitSplits(from: Int = 0, to: Int = 9): Array[Array[Byte]] = rangeSplits(from, to)

  // note: we don't include 0 to avoid an empty initial tablet
  private def hexSplits(): Array[Array[Byte]] =
    "123456789abcdefABCDEF".map(_.toString.getBytes(StandardCharsets.UTF_8)).toArray

  // note: we don't include 0 to avoid an empty initial tablet
  private def alphanumericSplits(): Array[Array[Byte]] =
    (('1' to '9') ++ ('a' to 'z') ++ ('A' to 'Z')).map(c => Array(c.toByte)).toArray

  private def rangeSplits(from: Int, to: Int): Array[Array[Byte]] =
    Array.tabulate(to - from + 1)(i => Array((from + i).toByte))
}
