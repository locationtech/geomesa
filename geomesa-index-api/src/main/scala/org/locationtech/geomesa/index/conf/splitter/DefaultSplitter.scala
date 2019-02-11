/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.splitter

import java.nio.charset.StandardCharsets
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.index.conf.TableSplitter
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey}
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.text.{DateParsing, KVPairParser}
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

/**
  * Default splitter implementation that creates splits based on configured user data
  */
class DefaultSplitter extends TableSplitter with LazyLogging {

  import DefaultSplitter._

  override def getSplits(sft: SimpleFeatureType, index: String, options: String): Array[Array[Byte]] =
    getSplits(sft, index, null, options)

  override def getSplits(sft: SimpleFeatureType,
                         index: String,
                         partition: String,
                         options: String): Array[Array[Byte]] = {
    val splits = Try(IndexId.id(index)).toOption.flatMap { id =>
      val opts = Option(options).map(KVPairParser.parse).getOrElse(Map.empty)
      id.name match {
        case IdIndex.name                 => Some(idBytes(opts))
        case Z3Index.name | XZ3Index.name => Some(z3Bytes(sft, Option(partition), opts))
        case Z2Index.name | XZ2Index.name => Some(z2Bytes(opts))
        case AttributeIndex.name          => Some(attributeBytes(sft, id.attributes.head, opts))
        case AttributeIndex.JoinIndexName => Some(attributeBytes(sft, id.attributes.head, opts))
        case _ => None
      }
    }
    splits.getOrElse { logger.warn(s"Unhandled index type $index"); Array(Array.empty[Byte]) }
  }
}

object DefaultSplitter {

  val Instance = new DefaultSplitter

  object Parser {

    val Z3MinDateOption = s"${Z3Index.name}.min"
    val Z3MaxDateOption = s"${Z3Index.name}.max"

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
        val configured = DefaultSplitter.patterns(s"${IdIndex.name}.pattern", options)
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
      val patterns = DefaultSplitter.patterns(s"${AttributeIndex.name}.$name.pattern", options)
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
          val converted = FastConverter.convert(d, classOf[Date])
          if (converted == null) {
            throw new IllegalArgumentException(s"Could not convert date '$d' for splits")
          }
          converted
        }
      }

      date(Z3MinDateOption).map(_.getTime) match {
        case None => Seq.empty
        case Some(min) =>
          val max = date(Z3MaxDateOption).map(_.getTime).getOrElse(System.currentTimeMillis())

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
      bitSplits(s"${Z3Index.name}.bits", options, 1)
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
      bitSplits(s"${Z2Index.name}.bits", options, 2)
  }

  private def idBytes(options: Map[String, String]): Array[Array[Byte]] =
    Parser.idSplits(options).map(_.getBytes(StandardCharsets.UTF_8)).toArray

  private def attributeBytes(sft: SimpleFeatureType,
                             attribute: String,
                             options: Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val descriptor = sft.getDescriptor(attribute)
    val binding = if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
    Parser.attributeSplits(attribute, binding, options).map(_.getBytes(StandardCharsets.UTF_8)).toArray
  }

  private def z3Bytes(sft: SimpleFeatureType,
                      partition: Option[String],
                      options: Map[String, String]): Array[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // if this is a time partition, update the options to include the min/max dates
    val period = sft.getZ3Interval
    val opts = partition.flatMap(p => Try(p.toShort).toOption) match {
      case None => options
      case Some(p) =>
        val date = DateParsing.format(BinnedTime.binnedTimeToDate(period).apply(BinnedTime(p, 1L)))
        options ++ Map(Parser.Z3MinDateOption -> date, Parser.Z3MaxDateOption -> date)
    }
    Parser.z3Splits(period, opts).map {
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
