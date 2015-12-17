/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import org.opengis.feature.simple.SimpleFeature

object MinMaxHelper {
  trait MinMaxDefaults[T] {
    def min: T
    def max: T
  }

  implicit object MinMaxDate extends MinMaxDefaults[Date] {
    override def min: Date = new Date(java.lang.Long.MAX_VALUE)
    override def max: Date = new Date(java.lang.Long.MIN_VALUE)
  }

  implicit object MinMaxLong extends MinMaxDefaults[java.lang.Long] {
    override def min: java.lang.Long = java.lang.Long.MAX_VALUE
    override def max: java.lang.Long = java.lang.Long.MIN_VALUE
  }

  implicit object MinMaxInt extends MinMaxDefaults[java.lang.Integer] {
    override def min: java.lang.Integer = java.lang.Integer.MAX_VALUE
    override def max: java.lang.Integer = java.lang.Integer.MIN_VALUE
  }

  implicit object MinMaxDouble extends MinMaxDefaults[java.lang.Double] {
    override def min: java.lang.Double = java.lang.Double.MAX_VALUE
    override def max: java.lang.Double = java.lang.Double.MIN_VALUE
  }

  implicit object MinMaxFloat extends MinMaxDefaults[java.lang.Float] {
    override def min: java.lang.Float = java.lang.Float.MAX_VALUE
    override def max: java.lang.Float = java.lang.Float.MIN_VALUE
  }
}

import org.locationtech.geomesa.utils.stats.MinMaxHelper._

/**
 * The MinMax stat merely returns the min/max of an attribute's values.
 * Works with dates, integers, longs, doubles, and floats.
 *
 * @param attrIndex attribute index for the attribute the histogram is being made for
 * @param attrType class type as a string for serialization purposes
 * @param min minimum value
 * @param max maximum value
 * @tparam T the type of the attribute the stat is targeting (needs to be comparable)
 */
class MinMax[T <: Comparable[T]](val attrIndex: Int, val attrType: String, var min: T, var max: T)
                                (implicit minMaxDefaults: MinMaxDefaults[T]) extends Stat {
  if (min == null || max == null) {
    throw new Exception("Null min or max encountered when creating MinMax class.") // shouldn't happen, but just to be safe
  }

  override def observe(sf: SimpleFeature): Unit = {
    val sfval = sf.getAttribute(attrIndex)

    if (sfval != null) {
      updateMin(sfval.asInstanceOf[T])
      updateMax(sfval.asInstanceOf[T])
    }
  }

  override def add(other: Stat): Stat = {
    other match {
      case mm: MinMax[T] =>
        updateMin(mm.min)
        updateMax(mm.max)
    }

    this
  }

  private def updateMin(sfval: T): Unit = {
    if (min.compareTo(sfval) > 0) {
      min = sfval
    }
  }

  private def updateMax(sfval: T): Unit = {
    if (max.compareTo(sfval) < 0) {
      max = sfval
    }
  }

  override def toJson(): String = s"""{ "min": $min, "max": $max }"""

  override def clear(): Unit = {
    min = minMaxDefaults.min
    max = minMaxDefaults.max
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[MinMax[T]] && {
      val minmax = obj.asInstanceOf[MinMax[T]]
      attrIndex == minmax.attrIndex &&
      attrType == minmax.attrType &&
      min == minmax.min &&
      max == minmax.max
    }
  }
}



