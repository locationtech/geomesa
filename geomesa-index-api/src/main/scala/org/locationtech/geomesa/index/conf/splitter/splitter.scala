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
import java.util.regex.Pattern
import java.util.{Date, Locale}
import javafx.scene.input.KeyCode

import com.google.common.base.Ascii
import com.google.common.primitives.{Bytes, Longs, Shorts}
import org.geotools.util.Converters
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

package object splitter {

  class DefaultSplitter extends TableSplitter {

    override def getSplits(index: String,
                           sft: SimpleFeatureType,
                           options: java.util.Map[String, String]): Array[Array[Byte]] = {
      // TODO make these constants
      Option(options.get(s"$index.type")).map(_.toLowerCase(Locale.US)) match {
        case Some("alphanumeric")  => alphanumericSplits(sft, options)
        case Some("digit")         => digitSplits(sft, options)
        case Some("hex")           => hexSplits(sft, options)
        case Some("z3")            => z3Splits(sft, options)
        case Some("z2")            => z2Splits(sft, options)
        case Some("attribute")     => attributeSplits(sft, options)
        case None if index == "id" => hexSplits(sft, options) // TODO defaults for other indices?
        case None | Some("none")   => Array.empty
        case Some(t) => throw new IllegalArgumentException(s"Unhandled split type '$t'")
      }
    }
  }

  // note: we don't include 0 to avoid an empty initial tablet
  private def hexSplits(sft: SimpleFeatureType, options: java.util.Map[String, String]): Array[Array[Byte]] =
    "123456789abcdefABCDEF".map(_.toString.getBytes(StandardCharsets.UTF_8)).toArray

  // note: we don't include 0 to avoid an empty initial tablet
  private def alphanumericSplits(sft: SimpleFeatureType, options: java.util.Map[String, String]): Array[Array[Byte]] = {
    // TODO allow for options
    (('1' to '9') ++ ('a' to 'z') ++ ('A' to 'Z')).map(c => Array(c.toByte)).toArray
  }

  /**
    * @param options allowed options are "fmt", "min", and "max"
    * @return
    */
  private def digitSplits(sft: SimpleFeatureType, options: java.util.Map[String, String]): Array[Array[Byte]] = {
    import scala.collection.JavaConversions._
    val fmt = options.getOrElse("fmt", "%01d")
    val min = options.getOrElse("min", "1").toInt
    val max = options.getOrElse("max", "9").toInt
    (min to max).map(fmt.format(_).getBytes(StandardCharsets.UTF_8)).toArray
  }

  private def z3Splits(sft: SimpleFeatureType, options: java.util.Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val min = options.get("min")
    val max = options.get("max")
    val minDate = Converters.convert(min, classOf[Date])
    var maxDate = Option(Converters.convert(max, classOf[Date]))
    if (min == null) {
      Array.empty
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

          val sfc = Z3SFC(sft.getZ3Interval)
          val minZ = sfc.index(sfc.lon.min, sfc.lat.min, sfc.time.min.toLong).z
          val maxZ = sfc.index(sfc.lon.max, sfc.lat.max, sfc.time.max.toLong).z

          // mask for zeroing the unused bits
          val zMask = Long.MaxValue << (64 - bits)
          // step needed (due to the mask) to bump up the z value for the next split
          val zStep = 1L << (64 - bits)

          val count = ((maxZ - minZ) / zStep).toInt
          val bitBytes = Array.tabulate(count)(i => Longs.toByteArray(minZ + i * zStep)) :+ Longs.toByteArray(maxZ)
          times.flatMap(time => bitBytes.map(Bytes.concat(time, _)))
      }
    }
  }

  private def z2Splits(sft: SimpleFeatureType, options: java.util.Map[String, String]): Array[Array[Byte]] = {
    // TODO
    Array.empty
  }

  def uppercaseSplits = ('A' to 'Z').map { c => Array(c.toByte) }.toArray
  def lowercaseSplits = ('a' to 'z').map { c => Array(c.toByte) }.toArray

  def splitsForStringAttribute(ad: AttributeDescriptor,
                               sft: SimpleFeatureType,
                               options: util.Map[String, String]): Array[Array[Byte]] = {
    options.get(s"${ad.getLocalName}.pattern") match {
      case "[:lower:]"           => lowercaseSplits

      case "[:lower:][:lower:]"  =>
        for {
          c <- lowercaseSplits
          d <- lowercaseSplits
        } yield {
          Bytes.concat(c, d)
        }

      case "[:upper:]"           => uppercaseSplits

      case "[:upper:][:upper:]"  =>
        for {
          c <- uppercaseSplits
          d <- uppercaseSplits
        } yield {
          Bytes.concat(c, d)
        }
    }
  }

  def splitsForIntAttribute(ad: AttributeDescriptor,
                               sft: SimpleFeatureType,
                               options: util.Map[String, String]): Array[Array[Byte]] = {
    ???
  }

  def splitsForAttribute(ad: AttributeDescriptor,
                         sft: SimpleFeatureType,
                         options: util.Map[String, String]): Array[Array[Byte]] = {
    ad.getType.getBinding match {
      case _: Class[String]            => splitsForStringAttribute(ad, sft, options)
      case _: Class[java.lang.Integer] => splitsForIntAttribute(ad, sft, options)
      case _: Class[java.lang.Long]    => splitsForIntAttribute(ad, sft, options)
    }
  }

  private def attributeSplits(sft: SimpleFeatureType, options: java.util.Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._
    import scala.collection.JavaConversions._

    sft.getAttributeDescriptors.filter(_.isIndexed).map { ad => splitsForAttribute(ad, sft, options) }.reduce(_ ++ _)
  }


}
