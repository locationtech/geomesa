/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang
import java.util.Date

import org.opengis.feature.simple.SimpleFeature

import scala.util.parsing.json.JSONObject

/**
 * This object provides the type classes used by the RangeHistogram.
 * BinAble provides methods which help bin a type's range of values into
 * equal-sized (or close to equal-sized in many cases) smaller ranges.
 */
object BinHelper {
  trait BinAble[T] {
    def getBinSize(numBins: Int, lowerEndpoint: T, upperEndpoint: T): T
    def getBinIndex(attributeValue: T, binSize: T, numBins: Int, lowerEndpoint: T, upperEndpoint: T): T
    def getBinKey(binSize: T, bucketIndex: Int, lowerEndpoint: T): T
  }

  implicit object BinAbleDate extends BinAble[Date] {
    override def getBinSize(numBins: Int, lowerEndpoint: Date, upperEndpoint: Date): Date = {
      new Date((upperEndpoint.getTime - lowerEndpoint.getTime) / numBins)
    }

    override def getBinIndex(attributeValue: Date,
                             binSize: Date,
                             numBins: Int,
                             lowerEndpoint: Date,
                             upperEndpoint: Date): Date = {
      if (!attributeValue.before(lowerEndpoint) && attributeValue.before(upperEndpoint)) {
        var bucketIndex = (attributeValue.getTime - lowerEndpoint.getTime) / binSize.getTime
        if (bucketIndex >= numBins)
          bucketIndex = numBins - 1

        getBinKey(binSize, bucketIndex.toInt, lowerEndpoint)
      } else {
        null
      }
    }

    override def getBinKey(binSize: Date, bucketIndex: Int, lowerEndpoint: Date): Date = {
      new Date(lowerEndpoint.getTime + (binSize.getTime * bucketIndex))
    }
  }

  implicit object BinAbleLong extends BinAble[lang.Long] {
    override def getBinSize(numBins: Int, lowerEndpoint: lang.Long, upperEndpoint: lang.Long): lang.Long = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def getBinIndex(attributeValue: lang.Long,
                             binSize: lang.Long,
                             numBins: Int,
                             lowerEndpoint: lang.Long,
                             upperEndpoint: lang.Long): lang.Long = {
      if (attributeValue >= lowerEndpoint && attributeValue < upperEndpoint) {
        var bucketIndex = (attributeValue - lowerEndpoint) / binSize
        if (bucketIndex >= numBins)
          bucketIndex = numBins - 1

        getBinKey(binSize, bucketIndex.toInt, lowerEndpoint)
      } else {
        null
      }
    }

    override def getBinKey(binSize: lang.Long, bucketIndex: Int, lowerEndpoint: lang.Long): lang.Long = {
      lowerEndpoint + (binSize * bucketIndex)
    }
  }

  implicit object BinAbleInteger extends BinAble[java.lang.Integer] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Integer, upperEndpoint: java.lang.Integer): java.lang.Integer = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def getBinIndex(attributeValue: Integer,
                             binSize: java.lang.Integer,
                             numBins: Int,
                             lowerEndpoint: Integer,
                             upperEndpoint: Integer): Integer = {
      if (attributeValue >= lowerEndpoint && attributeValue < upperEndpoint) {
        var bucketIndex = (attributeValue - lowerEndpoint) / binSize
        if (bucketIndex >= numBins)
          bucketIndex = numBins - 1

        getBinKey(binSize, bucketIndex, lowerEndpoint)
      } else {
        null
      }
    }

    override def getBinKey(binSize: java.lang.Integer, bucketIndex: Int, lowerEndpoint: lang.Integer): lang.Integer = {
      lowerEndpoint + (binSize * bucketIndex)
    }
  }

  implicit object BinAbleDouble extends BinAble[java.lang.Double] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Double, upperEndpoint: java.lang.Double): java.lang.Double = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def getBinIndex(attributeValue: lang.Double,
                             binSize: java.lang.Double,
                             numBins: Int,
                             lowerEndpoint: lang.Double,
                             upperEndpoint: lang.Double): lang.Double = {
      if (attributeValue >= lowerEndpoint && attributeValue < upperEndpoint) {
        var bucketIndex = (attributeValue - lowerEndpoint) / binSize
        if (bucketIndex >= numBins)
          bucketIndex = numBins - 1

        getBinKey(binSize, bucketIndex.toInt, lowerEndpoint)
      } else {
        null
      }
    }

    override def getBinKey(binSize: java.lang.Double, bucketIndex: Int, lowerEndpoint: lang.Double): lang.Double = {
      lowerEndpoint + (binSize * bucketIndex)
    }
  }

  implicit object BinAbleFloat extends BinAble[java.lang.Float] {
    override def getBinSize(numBins: Int, lowerEndpoint: java.lang.Float, upperEndpoint: java.lang.Float): java.lang.Float = {
      (upperEndpoint - lowerEndpoint) / numBins
    }

    override def getBinIndex(attributeValue: lang.Float,
                             binSize: java.lang.Float,
                             numBins: Int,
                             lowerEndpoint: lang.Float,
                             upperEndpoint: lang.Float): lang.Float = {
      if (attributeValue >= lowerEndpoint && attributeValue < upperEndpoint) {
        var bucketIndex = (attributeValue - lowerEndpoint) / binSize
        if (bucketIndex >= numBins)
          bucketIndex = numBins - 1

        getBinKey(binSize, bucketIndex.toInt, lowerEndpoint)
      } else {
        null
      }
    }

    override def getBinKey(binSize: java.lang.Float, bucketIndex: Int, lowerEndpoint: lang.Float): lang.Float = {
      lowerEndpoint + (binSize * bucketIndex)
    }
  }
}

import org.locationtech.geomesa.utils.stats.BinHelper._

/**
 * The range histogram's state is stored in a hashmap, where the keys are the bins and the values are the counts.
 * A bin is a range of values but is treated as the lower endpoint of the bin to persist in the hashmap.
 * A value is in a particular bin if it is equal to the bin value or less than the subsequent bin value.
 *
 * e.g. a range of 0 to 3 with 3 bins will result in these bins: [0, 1), [1, 2), [2, 3) and the hashmap will contain
 * keys 0, 1, and 2.
 *
 * @param attrIndex attribute index for the attribute the histogram is being made for
 * @param numBins number of bins the histogram has
 * @param lowerEndpoint lower end of histogram
 * @param upperEndpoint upper end of histogram
 * @tparam T a comparable type which must have a StatHelperFunctions type class
 */
class RangeHistogram[T: BinAble](val attrIndex: Int,
                                  val attrType: String,
                                  val numBins: Int,
                                  val lowerEndpoint: T,
                                  val upperEndpoint: T) extends Stat {

  override type S = RangeHistogram[T]

  val binHelper = implicitly[BinAble[T]]
  val binSize = binHelper.getBinSize(numBins, lowerEndpoint, upperEndpoint)

  val histogram = {
    val initial = new collection.mutable.HashMap[T, Long]()
    var i = 0
    while (i < numBins) {
      initial.put(binHelper.getBinKey(binSize, i, lowerEndpoint), 0)
      i +=1
    }
    initial
  }

  override def observe(sf: SimpleFeature): Unit = {
    val sfval = sf.getAttribute(attrIndex)
    if (sfval != null) {
      val binIndex = binHelper.getBinIndex(sfval.asInstanceOf[T], binSize, numBins, lowerEndpoint, upperEndpoint)
      if (binIndex != null) {
        histogram(binIndex) += 1
      }
    }
  }

  override def +=(other: RangeHistogram[T]): RangeHistogram[T] = {
    other.histogram.foreach { case (k, v) => histogram(k) += v }; this
  }

  override def toJson(): String = {
    val jsonMap = histogram.toMap.map { case (k, v) => k.toString -> v }
    new JSONObject(jsonMap).toString()
  }

  override def clear(): Unit = histogram.keys.foreach(bin => histogram(bin) = 0)

  override def equals(obj: Any): Boolean = {
    obj match {
      case rh: RangeHistogram[T] =>
        attrIndex == rh.attrIndex &&
            attrType == rh.attrType &&
            numBins == rh.numBins &&
            lowerEndpoint == rh.lowerEndpoint &&
            upperEndpoint == rh.upperEndpoint &&
            histogram == rh.histogram
      case _ => false
    }
  }
}
