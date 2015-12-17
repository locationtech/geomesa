/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import org.joda.time.format.DateTimeFormat
import org.locationtech.geomesa.utils.stats.MinMaxHelper._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.parsing.combinator.RegexParsers

/**
 * Stats used by the StatsIterator to compute various statistics server-side for a given query.
 */
trait Stat {
  /**
   * Compute statistics based upon the given simple feature.
   * This method will be called for every SimpleFeature a query returns.
   *
   * @param sf
   */
  def observe(sf: SimpleFeature): Unit

  /**
   * Meant to be used to combine two Stats of the same subtype.
   * Used in the "reduce" step client-side.
   *
   * @param other the other stat to add (this will always be a stat of the same sub-type unless something horrible happens)
   * @return the original stat with the other stat's data added
   */
  def add(other: Stat): Stat

  /**
   * Serves as serialization needed for storing the computed statistic in a SimpleFeature.
   *
   * @return stat serialized as a json string
   */
  def toJson(): String

  /**
   * Necessary method used by the StatIterator.
   * Leaving the isEmpty as false ensures that we will always get a stat back from the query
   * (even if the query doesn't hit any rows).
   *
   * @return boolean value
   */
  def isEmpty: Boolean = false

  /**
   * Clears the stat to its original state when first initialized.
   * Necessary method used by the StatIterator.
   */
  def clear(): Unit

  /**
   * Equals method to simplify testing
   *
   * @param obj object to compare
   * @return true if equal to obj
   */
  override def equals(obj: Any): Boolean
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
  class StatParser(sft: SimpleFeatureType) extends RegexParsers {
    val attributeNameRegex = """\w+""".r
    val numBinRegex = """[1-9][0-9]*""".r // any non-zero positive int
    val nonEmptyRegex = """[^,)]+""".r
    val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

    /**
     * Obtains the index of the attribute within the SFT
     *
     * @param attribute attribute name as a string
     * @return attribute index
     */
    private def getAttrIndex(attribute: String): Int = {
      val attrIndex = sft.indexOf(attribute)
      if (attrIndex == -1)
        throw new Exception(s"Invalid attribute name in stat string: $attribute")
      attrIndex
    }

    def minMaxParser: Parser[MinMax[_]] = {
      "MinMax(" ~> attributeNameRegex <~ ")" ^^ {
        case attribute =>
          val attrIndex = getAttrIndex(attribute)
          val attrType = sft.getType(attribute).getBinding
          val attrTypeString = attrType.getName
          attrType match {
            case _ if attrType == classOf[Date] =>
              new MinMax[Date](attrIndex, attrTypeString, MinMaxDate.min, MinMaxDate.max)
            case _ if attrType == classOf[java.lang.Long] =>
              new MinMax[java.lang.Long](attrIndex, attrTypeString, MinMaxLong.min, MinMaxLong.max)
            case _ if attrType == classOf[java.lang.Integer] =>
              new MinMax[java.lang.Integer](attrIndex, attrTypeString, MinMaxInt.min, MinMaxInt.max)
            case _ if attrType == classOf[java.lang.Double] =>
              new MinMax[java.lang.Double](attrIndex, attrTypeString, MinMaxDouble.min, MinMaxDouble.max)
            case _ if attrType == classOf[java.lang.Float] =>
              new MinMax[java.lang.Float](attrIndex, attrTypeString, MinMaxFloat.min, MinMaxFloat.max)
            case _ =>
              throw new Exception(s"Cannot create stat for invalid type: $attrType for attribute: $attribute")
          }
      }
    }

    def iteratorStackParser: Parser[IteratorStackCounter] = {
      "IteratorStackCounter" ^^ { case _ => new IteratorStackCounter() }
    }

    def enumeratedHistogramParser: Parser[EnumeratedHistogram[_]] = {
      "EnumeratedHistogram(" ~> attributeNameRegex <~ ")" ^^ {
        case attribute =>
          val attrIndex = getAttrIndex(attribute)
          val attrType = sft.getType(attribute).getBinding
          val attrTypeString = attrType.getName
          attrType match {
            case _ if attrType == classOf[Date] =>
              new EnumeratedHistogram[Date](attrIndex, attrTypeString)
            case _ if attrType == classOf[java.lang.Integer] =>
              new EnumeratedHistogram[java.lang.Integer](attrIndex, attrTypeString)
            case _ if attrType == classOf[java.lang.Long] =>
              new EnumeratedHistogram[java.lang.Long](attrIndex, attrTypeString)
            case _ if attrType == classOf[java.lang.Float] =>
              new EnumeratedHistogram[java.lang.Float](attrIndex, attrTypeString)
            case _ if attrType == classOf[java.lang.Double] =>
              new EnumeratedHistogram[java.lang.Double](attrIndex, attrTypeString)
            case _ =>
              throw new Exception(s"Cannot create stat for invalid type: $attrType for attribute: $attribute")
          }
      }
    }

    def rangeHistogramParser: Parser[RangeHistogram[_]] = {
      "RangeHistogram(" ~> attributeNameRegex ~ "," ~ numBinRegex ~ "," ~ nonEmptyRegex ~ "," ~ nonEmptyRegex <~ ")" ^^ {
        case attribute ~ "," ~ numBins ~ "," ~ lowerEndpoint ~ "," ~ upperEndpoint =>
          val attrIndex = getAttrIndex(attribute)
          val attrType = sft.getType(attribute).getBinding
          val attrTypeString = attrType.getName
          attrType match {
            case _ if attrType == classOf[Date] =>
              new RangeHistogram[Date](attrIndex, attrTypeString, numBins.toInt,
                StatHelpers.dateFormat.parseDateTime(lowerEndpoint).toDate, StatHelpers.dateFormat.parseDateTime(upperEndpoint).toDate)
            case _ if attrType == classOf[java.lang.Integer] =>
              new RangeHistogram[java.lang.Integer](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toInt, upperEndpoint.toInt)
            case _ if attrType == classOf[java.lang.Long] =>
              new RangeHistogram[java.lang.Long](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toLong, upperEndpoint.toLong)
            case _ if attrType == classOf[java.lang.Double] =>
              new RangeHistogram[java.lang.Double](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toDouble, upperEndpoint.toDouble)
            case _ if attrType == classOf[java.lang.Float] =>
              new RangeHistogram[java.lang.Float](attrIndex, attrTypeString, numBins.toInt, lowerEndpoint.toFloat, upperEndpoint.toFloat)
            case _ =>
              throw new Exception(s"Cannot create stat for invalid type: $attrType for attribute: $attribute")
          }
      }
    }

    def statParser: Parser[Stat] = {
      minMaxParser |
        iteratorStackParser |
        enumeratedHistogramParser |
        rangeHistogramParser
    }

    def statsParser: Parser[Stat] = {
      rep1sep(statParser, ";") ^^ {
        case statParsers: Seq[Stat] =>
          if (statParsers.length == 1) statParsers.head else new SeqStat(statParsers)
      }
    }

    def parse(s: String): Stat = {
      parseAll(statsParser, s) match {
        case Success(result, _) => result
        case failure: NoSuccess =>
          throw new Exception(s"Could not parse the stats string: $s")
      }
    }
  }

  def apply(sft: SimpleFeatureType, s: String) = new StatParser(sft).parse(s)
}
