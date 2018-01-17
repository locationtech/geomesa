/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.splitter

import java.util.Date

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.util.Converters
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.index.conf.TableSplitter
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.index.index.{AttributeIndex, IdIndex}
import org.locationtech.geomesa.utils.text.KVPairParser
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Default splitter implementation that creates splits based on configured user data
  */
class DefaultSplitter extends TableSplitter with LazyLogging {

  import DefaultSplitter._

  override def getSplits(sft: SimpleFeatureType,
                         index: String,
                         options: String): Array[Array[Byte]] = {
    val opts = Option(options).map(KVPairParser.parse).getOrElse(Map.empty)
    index match {
      case IdIndex.Name                 => idSplits(opts)
      case AttributeIndex.Name          => attributeSplits(sft, opts)
      case Z3Index.Name | XZ3Index.Name => z3Splits(sft, opts)
      case Z2Index.Name | XZ2Index.Name => z2Splits(opts)
      case _ => logger.warn(s"Unhandled index type $index"); Array(Array.empty[Byte])
    }
  }
}

object DefaultSplitter {

  /**
    * Creates splits suitable for a feature ID index. If nothing is specified, will assume a hex distribution.
    *
    * *  Options are:
    *
    * * 'id.pattern' - pattern used
    *
    * Additional patterns can be specified with 'id.pattern2', etc
    *
    * @param options user data containing split configuration
    * @return
    */
  private def idSplits(options: Map[String, String]): Array[Array[Byte]] = {
    val option = s"${IdIndex.Name}.pattern"
    val patterns = if (options.contains(option)) {
      Iterator.single(options(option)) ++
          Iterator.range(2, Int.MaxValue).map(i => options.get(s"$option$i").orNull)
    } else {
      Iterator("[0]", "[4]", "[8]", "[c]") // 4 splits assuming hex layout
    }
    patterns.takeWhile(_ != null).flatMap(SplitPatternParser.parse).map(stringPatternSplits).reduceLeft(_ ++ _)
  }

  /**
    * Creates splits suitable for an attribute index. Each indexed attribute can be split individually.
    *
    *  Options are:
    *
    * * 'attr.&lt;attribute&gt;.pattern' - pattern used for a given attribute
    *
    * Additional patterns can be specified with 'attr.&lt;attribute&gt;.pattern2', etc
    *
    * @param sft simple feature type being configured
    * @param options user data containing split configuration
    * @return
    */
  private def attributeSplits(sft: SimpleFeatureType, options: Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._

    import scala.collection.JavaConversions._

    sft.getAttributeDescriptors.collect { case ad if ad.isIndexed =>
      val binding = ad.getType.getBinding
      val option = s"${AttributeIndex.Name}.${ad.getLocalName}.pattern"
      val patternIterator = Iterator.single(options.get(option).orNull) ++
          Iterator.range(2, Int.MaxValue).map(i => options.get(s"$option$i").orNull)
      val patterns = patternIterator.takeWhile(_ != null).toSeq
      val ranges = patterns.flatMap(SplitPatternParser.parse)
      val splits = if (classOf[Number].isAssignableFrom(binding)) {
        try {
          ranges.map(numberPatternSplits(_, binding))
        } catch {
          case e: NumberFormatException =>
            throw new IllegalArgumentException(s"Trying to create splits for attribute '${ad.getLocalName}' " +
                s"of type ${ad.getType.getBinding.getSimpleName}, but splits could not be parsed as a number: " +
                patterns.mkString(" "), e)
        }
      } else {
        ranges.map(stringPatternSplits)
      }
      splits.reduceLeftOption(_ ++ _).getOrElse(Array.empty)
    }.reduce(_ ++ _)
  }

  /**
    * Creates splits suitable for a z3 index. Generally, a split will be created per time interval (e.g. week).
    * Further splits can be created by specifying the number of bits that will be used for splits, per week.
    * The total number of splits is: (number of time intervals) * 2^^(number of bits)
    *
    *  Options are:
    *
    * * 'z3.min' - min date for data
    * * 'z3.max' - max date for data (must also include min date) - will default to current date if not specified
    * * 'z3.bits' - number of bits used to create splits. e.g. 2 bits will create 4 splits.
    *
    * @param sft simple feature type being configured
    * @param options user data containing split configuration
    * @return
    */
  private def z3Splits(sft: SimpleFeatureType, options: Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val min = options.get(s"${Z3Index.Name}.min")
    val max = options.get(s"${Z3Index.Name}.max")
    val minDate = min.map(Converters.convert(_, classOf[Date])).orNull
    val maxDate = max.flatMap(m => Option(Converters.convert(m, classOf[Date])))
    if (min.isEmpty) {
      Array(Array.empty)
    } else if (minDate == null || (max.isDefined && maxDate.isEmpty)) {
      throw new IllegalArgumentException(s"Could not convert dates '$min/$max' for splits")
    } else {
      val toBin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
      val minBin = toBin(minDate.getTime).bin
      val maxBin = toBin(maxDate.map(_.getTime).getOrElse(System.currentTimeMillis())).bin
      val times = Array.tabulate(maxBin - minBin + 1)(i => Shorts.toByteArray((minBin + i).toShort))
      options.get(s"${Z3Index.Name}.bits").map(_.toInt) match {
        case None => times
        case Some(bits) =>
          // note: first bit in z value is not used, and is always 0
          val zBytes = bitSplit(bits, 1)
          for (time <- times; z <- zBytes) yield { Bytes.concat(time, z) }
      }
    }
  }

  /**
    * Creates splits suitable for a z2 index. Splits can be created by specifying the number of bits that will be
    * used. The total number of splits is: 2^^(number of bits)
    *
    *  Options are:
    *
    * * 'z2.bits' - number of bits used to create splits. e.g. 2 bits will create 4 splits.
    *
    * @param options user data containing split configuration
    * @return
    */
  private def z2Splits(options: Map[String, String]): Array[Array[Byte]] = {
    // note: first 2 bits in z value are not used, and are always 0
    options.get(s"${Z2Index.Name}.bits").map(b => bitSplit(b.toInt, 2)).getOrElse(Array(Array.empty))
  }

  /**
    * Create splits based on individual bits. Will create 2^^(bits) splits.
    *
    * @param bits number of bits to consider
    * @param maskedBits number of unused bits at the start of the first byte
    * @return
    */
  private def bitSplit(bits: Int, maskedBits: Int): Array[Array[Byte]] = {
    require(bits > 0 && bits < 64, "Bit split must be between 1 and 63")

    // recursive function to create all bit permutations
    def add(result: Seq[Seq[Int]], bits: Seq[Int], remaining: Int): Seq[Seq[Int]] = {
      if (remaining == 0) { Seq(bits) } else {
        val zero = add(result, bits :+ 0, remaining - 1)
        val one = add(result, bits :+ 1, remaining - 1)
        result ++ zero ++ one
      }
    }
    // note: first bit(s) in z value are not used, and are always 0
    def toBytes(bits: Seq[Int]): Array[Byte] = {
      val binaryString = (Seq.fill(maskedBits)(0) ++ bits).padTo(64, 0).mkString("")
      Longs.toByteArray(java.lang.Long.parseLong(binaryString, 2))
    }

    add(Seq.empty, Seq.empty, bits).map(toBytes).toArray
  }

  private def stringPatternSplits(range: (String, String)): Array[Array[Byte]] = {
    (0 until range._1.length).map(i => rangeSplits(range._1.charAt(i), range._2.charAt(i))).reduceLeft {
      (left, right) => for (a <- left; b <- right) yield { Bytes.concat(a, b) }
    }
  }

  @throws(classOf[NumberFormatException])
  private def numberPatternSplits(range: (String, String), binding: Class[_]): Array[Array[Byte]] = {
    // recursive function to create all number permutations
    def add(result: Seq[String], value: String, remaining: Seq[(Int, Int)]): Seq[String] = {
      if (remaining.isEmpty) { Seq(value) } else {
        val (start, end) = remaining.head
        result ++ (start to end).flatMap(i => add(result, value + i, remaining.tail))
      }
    }

    val remaining = (0 until range._1.length).map { i =>
      (Integer.parseInt(range._1(i).toString), Integer.parseInt(range._2(i).toString))
    }
    val splits = add(Seq.empty, "", remaining)
    splits.map(AttributeIndex.encodeForQuery(_, binding)).toArray
  }

  /**
    * Splits from one char to a second (inclusive)
    *
    * @param from from
    * @param to to
    * @return
    */
  private def rangeSplits(from: Char, to: Char): Array[Array[Byte]] =
    Array.range(from, to + 1).map(b => Array(b.toByte))
}
