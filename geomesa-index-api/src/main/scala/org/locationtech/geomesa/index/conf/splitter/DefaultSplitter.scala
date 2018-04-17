/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.splitter

import java.nio.charset.StandardCharsets
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.util.Converters
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.conf.TableSplitter
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey}
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.utils.index.ByteArrays
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
      case IdIndex.Name                 => idBytes(opts)
      case AttributeIndex.Name          => attributeBytes(sft, opts)
      case Z3Index.Name | XZ3Index.Name => z3Bytes(sft, opts)
      case Z2Index.Name | XZ2Index.Name => z2Bytes(opts)
      case _ => logger.warn(s"Unhandled index type $index"); Array(Array.empty[Byte])
    }
  }
}

object DefaultSplitter {

  object Parser {

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
    def idSplits(options: Map[String, String]): Seq[String] = {
      val patterns = {
        val configured = DefaultSplitter.patterns(s"${IdIndex.Name}.pattern", options)
        if (configured.hasNext) { configured } else {
          Iterator("[0]", "[4]", "[8]", "[c]") // 4 splits assuming hex layout
        }
      }
      patterns.flatMap(SplitPatternParser.parse).map(stringPatternSplits).reduceLeft(_ ++ _)
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
      * @param name attribute name
      * @param binding attribute type
      * @param options user data containing split configuration
      * @return
      */
    def attributeSplits(name: String, binding: Class[_], options: Map[String, String]): Seq[String] = {
      val patterns = DefaultSplitter.patterns(s"${AttributeIndex.Name}.$name.pattern", options)
      val ranges = patterns.flatMap(SplitPatternParser.parse)
      val splits = if (classOf[Number].isAssignableFrom(binding)) {
        try {
          ranges.map(numberPatternSplits(_, binding))
        } catch {
          case e: NumberFormatException =>
            throw new IllegalArgumentException(s"Trying to create splits for attribute '$name' " +
                s"of type ${binding.getName}, but splits could not be parsed as a number: " +
                patterns.mkString(" "), e)
        }
      } else {
        ranges.map(stringPatternSplits)
      }
      splits.reduceLeftOption(_ ++ _).getOrElse(Seq.empty)
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
      * @param interval interval of the z3 index
      * @param options user data containing split configuration
      * @return
      */
    def z3Splits(interval: TimePeriod, options: Map[String, String]): Seq[(Short, Option[Long])] = {

      def date(key: String): Option[Date] = {
        options.get(key).map { d =>
          val converted = Converters.convert(d, classOf[Date])
          if (converted == null) {
            throw new IllegalArgumentException(s"Could not convert date '$d' for splits")
          }
          converted
        }
      }

      date(s"${Z3Index.Name}.min").map(_.getTime) match {
        case None => Seq.empty
        case Some(min) =>
          val max = date(s"${Z3Index.Name}.max").map(_.getTime).getOrElse(System.currentTimeMillis())

          val toBin = BinnedTime.timeToBinnedTime(interval)
          val minBin = toBin(min).bin
          val maxBin = toBin(max).bin
          val times = Seq.range(minBin, maxBin + 1).map(_.toShort)

          val zs = z3BitSplits(options)
          if (zs.isEmpty) {
            times.map(t => (t, None))
          } else {
            times.flatMap(t => zs.map(z => (t, Some(z))))
          }
      }
    }

    /**
      * Creates just the bit-splits for a z3 index. Generally this would be applied per time interval. Use `z3Splits`
      * to get bits per time interval.
      *
      * The total number of splits is: 2^^(number of bits)
      *
      *  Options are:
      *
      * * 'z3.bits' - number of bits used to create splits. e.g. 2 bits will create 4 splits.
      *
      * @param options user data containing split configuration
      * @return
      */
    def z3BitSplits(options: Map[String, String]): Seq[Long] = {
      // note: first bit in z value is not used, and is always 0
      bitSplits(s"${Z3Index.Name}.bits", options, 1)
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
    def z2Splits(options: Map[String, String]): Seq[Long] =
      // note: first 2 bits in z value are not used, and are always 0
      bitSplits(s"${Z2Index.Name}.bits", options, 2)
  }

  private def idBytes(options: Map[String, String]): Array[Array[Byte]] =
    Parser.idSplits(options).map(_.getBytes(StandardCharsets.UTF_8)).toArray

  private def attributeBytes(sft: SimpleFeatureType, options: Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._

    import scala.collection.JavaConversions._

    sft.getAttributeDescriptors.collect { case ad if ad.isIndexed =>
      val splits = Parser.attributeSplits(ad.getLocalName, ad.getType.getBinding, options)
      splits.map(_.getBytes(StandardCharsets.UTF_8)).toArray
    }.reduce(_ ++ _)
  }

  private def z3Bytes(sft: SimpleFeatureType, options: Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    Parser.z3Splits(sft.getZ3Interval, options).map {
      case (bin, None) => ByteArrays.toBytes(bin)
      case (bin, Some(z)) => ByteArrays.toBytes(bin, z)
    }.toArray
  }

  private def z2Bytes(options: Map[String, String]): Array[Array[Byte]] =
    Parser.z2Splits(options).map(ByteArrays.toBytes).toArray

  private def patterns(base: String, options: Map[String, String]): Iterator[String] = {
    val keys = Iterator.single(base) ++ Iterator.range(2, Int.MaxValue).map(i => s"$base$i")
    keys.map(options.get(_).orNull).takeWhile(_ != null)
  }

  private def stringPatternSplits(range: (String, String)): Seq[String] = {
    (0 until range._1.length).map(i => rangeSplits(range._1.charAt(i), range._2.charAt(i))).reduceLeft {
      (left, right) => for (a <- left; b <- right) yield { a + b }
    }
  }

  @throws(classOf[NumberFormatException])
  private def numberPatternSplits(range: (String, String), binding: Class[_]): Seq[String] = {
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
    splits.map(AttributeIndexKey.encodeForQuery(_, binding))
  }

  /**
    * Splits from one char to a second (inclusive)
    *
    * @param from from
    * @param to to
    * @return
    */
  private def rangeSplits(from: Char, to: Char): Seq[String] = Seq.range(from, to + 1).map(_.toChar.toString)

  /**
    * Create splits based on individual bits. Will create 2^^(bits) splits.
    *
    * @param key key for number of bits to consider
    * @param options config options
    * @param maskedBits number of unused bits at the start of the first byte
    * @return
    */
  private def bitSplits(key: String, options: Map[String, String], maskedBits: Int): Seq[Long] = {
    options.get(key).map(_.toInt) match {
      case None => Seq.empty
      case Some(bits) =>
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
        def toLong(bits: Seq[Int]): Long = {
          val binaryString = (Seq.fill(maskedBits)(0) ++ bits).padTo(64, 0).mkString("")
          java.lang.Long.parseLong(binaryString, 2)
        }

        add(Seq.empty, Seq.empty, bits).map(toLong)
    }
  }
}
