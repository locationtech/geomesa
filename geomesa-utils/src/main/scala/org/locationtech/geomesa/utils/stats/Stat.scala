/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.lang.StringEscapeUtils
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.text.{EnhancedTokenParsers, WKTUtils}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.reflect.ClassTag
import scala.util.parsing.combinator.RegexParsers

/**
 * Stats used by the StatsIterator to compute various statistics server-side for a given query.
 */
trait Stat {

  type S <: Stat

  /**
   * Compute statistics based upon the given simple feature.
   * This method will be called for every SimpleFeature a query returns.
   *
   * @param sf feature to evaluate
   */
  def observe(sf: SimpleFeature): Unit

  /**
    * Tries to remove the given simple feature from the compiled statistics.
    * Note: may not be possible to un-observe a feature, in which case this method will
    * have no effect.
    *
    * @param sf feature to un-evaluate
    */
  def unobserve(sf: SimpleFeature): Unit

  /**
   * Add another stat to this stat. Avoids allocating another object.
   *
   * @param other the other stat to add
   */
  def +=(other: S): Unit

  /**
   * Non type-safe add - if stats are not the same type, will throw an exception
   *
   * @param other the other stat to add
   */
  def +=(other: Stat)(implicit d: DummyImplicit): Unit = this += other.asInstanceOf[S]

  /**
    * Combine two stats into a new stat
    *
    * @param other the other stat to add
    */
  def +(other: S): S

  /**
    * Non type-safe add - if stats are not the same type, will throw an exception
    *
    * @param other the other stat to add
    */
  def +(other: Stat)(implicit d: DummyImplicit): Stat = this + other.asInstanceOf[S]

  /**
   * Returns a json representation of the stat
   *
   * @return stat as a json string
   */
  def toJson: String

  /**
   * Necessary method used by the StatIterator. Indicates if the stat has any values or not
   *
   * @return true if stat contains values
   */
  def isEmpty: Boolean

  /**
    * Compares the two stats for equivalence. We don't use standard 'equals' as it gets messy with
    * mutable state and hash codes
    *
    * @param other other stat to compare
    * @return true if equals
    */
  def isEquivalent(other: Stat): Boolean

  /**
   * Clears the stat to its original state when first initialized.
   * Necessary method used by the StatIterator.
   */
  def clear(): Unit
}

/**
 * This class contains parsers which dictate how to instantiate a particular Stat.
 * Stats are created by passing a stats string as a query hint (QueryHints.STATS_STRING).
 *
 * A valid stats string should adhere to the parsers here:
 * e.g. "MinMax(attributeName);IteratorCount" or "RangeHistogram(attributeName,10,0,100)"
 * (see tests for more use cases)
 */
object Stat {

  def apply(sft: SimpleFeatureType, s: String) = new StatParser(sft).parse(s)

  /**
    * String that will be parsed to a count stat
    *
    * @return
    */
  def Count(): String = "Count()"

  /**
    * String that will be parsed to a min/max stat
    *
    * @param attribute attribute name to min/max
    * @return
    */
  def MinMax(attribute: String): String = s"MinMax(${safeString(attribute)})"

  /**
    * String that will be parsed to a histogram stat
    *
    * @param attribute attribute name to histogram
    * @return
    */
  def Histogram(attribute: String): String = s"Histogram(${safeString(attribute)})"

  /**
    * String that will be parsed into a count min sketch stat
    *
    * @param attribute attribute to sketch
    * @param precision precision of the sketch - @see Frequency
    * @return
    */
  def Frequency(attribute: String, precision: Int): String = s"Frequency(${safeString(attribute)},$precision)"

  /**
    * String that will be parsed into a z3 count min sketch stat
    *
    * @param geom geometry attribute
    * @param dtg date attribute
    * @param precision precision of the z value - @see FrequencyZ3
    * @return
    */
  def Z3Frequency(geom: String, dtg: String, precision: Int): String =
    s"Z3Frequency(${safeString(geom)},${safeString(dtg)},$precision)"

  /**
    * String that will be parsed to a binned histogram stat
    *
    * @param attribute attribute name to histogram
    * @param bins the number of bins to create
    * @param min min value for the histogram
    * @param max max value for the histogram
    * @tparam T class type of the histogram attribute
    * @return
    */
  def RangeHistogram[T](attribute: String, bins: Int, min: T, max: T)(implicit ct: ClassTag[T]): String = {
    val stringify = stringifier(ct.runtimeClass)
    s"RangeHistogram(${safeString(attribute)},$bins,${safeString(stringify(min))},${safeString(stringify(max))})"
  }

  /**
    * String that will be parsed into a z3 range histogram stat
    *
    * @param geom geometry attribute
    * @param dtg date attribute
    * @param length number of the bins per week - @see RangeHistogramZ3
    * @return
    */
  def Z3RangeHistogram(geom: String, dtg: String, length: Int): String =
    s"Z3RangeHistogram(${safeString(geom)},${safeString(dtg)},$length)"

  /**
    * String that will be parsed to a iterator stack counter
    *
    * @return
    */
  def IteratorStackCount(): String = "IteratorStackCount()"

  /**
    * String that will be parsed to a sequence of stat
    *
    * @param stats input strings that will be parsed as individual stats
    * @return
    */
  def SeqStat(stats: Seq[String]): String = stats.mkString(";")

  // note: adds quotes around the string
  private def safeString(s: String): String = s""""${StringEscapeUtils.escapeJava(s)}""""

  /**
    * Converts a value to a string
    *
    * @param clas class of the input value
    * @param json will the result be used in json? will quote appropriately if so
    * @tparam T type of input
    * @return
    */
  def stringifier[T](clas: Class[T], json: Boolean = false): Any => String = {
    val toString: (Any) => String = if (classOf[Geometry].isAssignableFrom(clas)) {
      (v) => WKTUtils.write(v.asInstanceOf[Geometry])
    } else if (clas == classOf[Date]) {
      (v) => GeoToolsDateFormat.print(v.asInstanceOf[Date].getTime)
    } else {
      (v) => v.toString
    }

    // add quotes to json strings if needed
    if (json && !classOf[Number].isAssignableFrom(clas)) {
      (v) => if (v == null) "null" else s""""${toString(v)}""""
    } else {
      (v) => if (v == null) "null" else toString(v)
    }
  }

  /**
    * Converts a string back to a value
    *
    * @param clas class of the stringified value
    * @tparam T type of the value class
    * @return
    */
  def destringifier[T](clas: Class[T]): String => T =
    if (clas == classOf[String]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.asInstanceOf[T]
    } else if (clas == classOf[Integer]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toInt.asInstanceOf[T]
    } else if (clas == classOf[jLong]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toLong.asInstanceOf[T]
    } else if (clas == classOf[jFloat]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toFloat.asInstanceOf[T]
    } else if (clas == classOf[jDouble]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toDouble.asInstanceOf[T]
    } else if (classOf[Geometry].isAssignableFrom(clas)) {
      (s) => if (s == "null") null.asInstanceOf[T] else WKTUtils.read(s).asInstanceOf[T]
    } else if (clas == classOf[Date]) {
      (s) => if (s == "null") null.asInstanceOf[T] else GeoToolsDateFormat.parseDateTime(s).toDate.asInstanceOf[T]
    } else {
      throw new RuntimeException(s"Unexpected class binding for stat attribute: $clas")
    }

  /**
    * Parser for stat strings
    *
    * @param sft simple feature type being evaluated
    */
  class StatParser(sft: SimpleFeatureType) extends RegexParsers with EnhancedTokenParsers {

    /**
     * Obtains the index of the attribute within the SFT
     *
     * @param attribute attribute name as a string
     * @return attribute index
     */
    private def getAttrIndex(attribute: String): Int = {
      val i = sft.indexOf(attribute)
      require(i != -1, s"Attribute '$attribute' does not exist in sft ${DataUtilities.encodeType(sft)}")
      i
    }

    val positiveInt = """[1-9][0-9]*""".r // any non-zero positive int
    val argument = dequotedString | "[a-zA-Z0-9_]+".r

    def countParser: Parser[CountStat] = {
      "Count()" ^^ { _ => new CountStat() }
    }

    def minMaxParser: Parser[MinMax[_]] = {
      "MinMax(" ~> argument <~ ")" ^^ {
        case attribute =>
          val attrIndex = getAttrIndex(attribute)
          val attrType = sft.getDescriptor(attribute).getType.getBinding

          if (attrType == classOf[String]) {
            new MinMax[String](attrIndex)
          } else if (attrType == classOf[Date]) {
            new MinMax[Date](attrIndex)
          } else if (attrType == classOf[jLong]) {
            new MinMax[jLong](attrIndex)
          } else if (attrType == classOf[Integer]) {
            new MinMax[Integer](attrIndex)
          } else if (attrType == classOf[jDouble]) {
            new MinMax[jDouble](attrIndex)
          } else if (attrType == classOf[jFloat]) {
            new MinMax[jFloat](attrIndex)
          } else if (classOf[Geometry].isAssignableFrom(attrType)) {
            new MinMax[Geometry](attrIndex)
          } else {
            println(s"\n\nCannot create stat for invalid type: $attrType for attribute: $attribute\n\n")
            throw new Exception(s"Cannot create stat for invalid type: $attrType for attribute: $attribute")
          }
      }
    }

    def iteratorStackParser: Parser[IteratorStackCount] = {
      "IteratorStackCount()" ^^ { case _ => new IteratorStackCount() }
    }

    def histogramParser: Parser[Histogram[_]] = {
      "Histogram(" ~> argument <~ ")" ^^ {
        case attribute =>
          val attrIndex = getAttrIndex(attribute)
          val attrType = sft.getDescriptor(attribute).getType.getBinding

          if (attrType == classOf[String]) {
            new Histogram[String](attrIndex)
          } else if (attrType == classOf[Date]) {
            new Histogram[Date](attrIndex)
          } else if (attrType == classOf[Integer]) {
            new Histogram[Integer](attrIndex)
          } else if (attrType == classOf[jLong]) {
            new Histogram[jLong](attrIndex)
          } else if (attrType == classOf[jFloat]) {
            new Histogram[jFloat](attrIndex)
          } else if (attrType == classOf[jDouble]) {
            new Histogram[jDouble](attrIndex)
          } else if (classOf[Geometry].isAssignableFrom(attrType )) {
            new Histogram[Geometry](attrIndex)
          } else {
            throw new Exception(s"Cannot create stat for invalid type: $attrType for attribute: $attribute")
          }
      }
    }

    def rangeHistogramParser: Parser[RangeHistogram[_]] = {
      "RangeHistogram(" ~> argument ~ "," ~ positiveInt ~ "," ~ argument ~ "," ~ argument <~ ")" ^^ {
        case attribute ~ "," ~ numBins ~ "," ~ lower ~ "," ~ upper =>
          val attrIndex = getAttrIndex(attribute)
          val attrType = sft.getDescriptor(attribute).getType.getBinding

          if (attrType == classOf[String]) {
            new RangeHistogram(attrIndex, numBins.toInt, (lower, upper))
          } else if (attrType == classOf[Date]) {
            val lowerDate = GeoToolsDateFormat.parseDateTime(lower).toDate
            val upperDate = GeoToolsDateFormat.parseDateTime(upper).toDate
            new RangeHistogram(attrIndex, numBins.toInt, (lowerDate, upperDate))
          } else if (attrType == classOf[Integer]) {
            new RangeHistogram(attrIndex, numBins.toInt, (Integer.valueOf(lower), Integer.valueOf(upper)))
          } else if (attrType == classOf[jLong]) {
            new RangeHistogram(attrIndex, numBins.toInt, (jLong.valueOf(lower), jLong.valueOf(upper)))
          } else if (attrType == classOf[jDouble]) {
            new RangeHistogram(attrIndex, numBins.toInt, (jDouble.valueOf(lower), jDouble.valueOf(upper)))
          } else if (attrType == classOf[jFloat]) {
            new RangeHistogram(attrIndex, numBins.toInt, (jFloat.valueOf(lower), jFloat.valueOf(upper)))
          } else if (classOf[Geometry].isAssignableFrom(attrType )) {
            new RangeHistogram(attrIndex, numBins.toInt, (WKTUtils.read(lower), WKTUtils.read(upper)))
          } else {
            throw new Exception(s"Cannot create stat for invalid type: $attrType for attribute: $attribute")
          }
      }
    }

    def z3RangeHistogramParser: Parser[Z3RangeHistogram] = {
      "Z3RangeHistogram(" ~> argument ~ "," ~ argument ~ "," ~ positiveInt <~ ")" ^^ {
        case geom ~ "," ~ dtg ~ "," ~ length =>
          new Z3RangeHistogram(getAttrIndex(geom), getAttrIndex(dtg), length.toInt)
      }
    }

    def frequencyParser: Parser[Frequency[_]] = {
      "Frequency(" ~> argument ~ "," ~ positiveInt <~ ")" ^^ {
        case attribute ~ "," ~ precision =>
          val attrIndex = getAttrIndex(attribute)
          val attrType = sft.getDescriptor(attribute).getType.getBinding

          if (attrType == classOf[String]) {
            new Frequency[String](attrIndex, precision.toInt)
          } else if (attrType == classOf[Date]) {
            new Frequency[Date](attrIndex, precision.toInt)
          } else if (attrType == classOf[Integer]) {
            new Frequency[Integer](attrIndex, precision.toInt)
          } else if (attrType == classOf[jLong]) {
            new Frequency[jLong](attrIndex, precision.toInt)
          } else if (attrType == classOf[jDouble]) {
            new Frequency[jDouble](attrIndex, precision.toInt)
          } else if (attrType == classOf[jFloat]) {
            new Frequency[jFloat](attrIndex, precision.toInt)
          } else if (classOf[Geometry].isAssignableFrom(attrType )) {
            new Frequency[Geometry](attrIndex, precision.toInt)
          } else {
            throw new Exception(s"Cannot create stat for invalid type: $attrType for attribute: $attribute")
          }
      }
    }

    def z3FrequencyParser: Parser[Z3Frequency] = {
      "Z3Frequency(" ~> argument ~ "," ~ argument ~ "," ~ positiveInt <~ ")" ^^ {
        case geom ~ "," ~ dtg ~ "," ~ precision =>
          new Z3Frequency(getAttrIndex(geom), getAttrIndex(dtg), precision.toInt)
      }
    }

    def statParser: Parser[Stat] = countParser | minMaxParser | iteratorStackParser | histogramParser |
        rangeHistogramParser | frequencyParser | z3RangeHistogramParser | z3FrequencyParser

    def statsParser: Parser[Stat] = {
      rep1sep(statParser, ";") ^^ {
        case statParsers: Seq[Stat] => if (statParsers.length == 1) statParsers.head else new SeqStat(statParsers)
      }
    }

    def parse(s: String): Stat = {
      parseAll(statsParser, s) match {
        case Success(result, _) => result
        case failure: NoSuccess =>
          throw new Exception(s"Could not parse the stats string: $s\n${failure.msg}")
      }
    }
  }
}

trait ImmutableStat extends Stat {

  override def observe(sf: SimpleFeature): Unit = fail()

  override def unobserve(sf: SimpleFeature): Unit = fail()

  override def +=(other: S): Unit = fail()

  override def clear(): Unit = fail()

  private def fail(): Unit = throw new RuntimeException("This stat is immutable")
}