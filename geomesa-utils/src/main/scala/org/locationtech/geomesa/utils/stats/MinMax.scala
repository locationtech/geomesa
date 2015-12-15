/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

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
case class MinMax[T <: Comparable[T]](attrIndex: Int, attrType: String, var min: T, var max: T) extends Stat {
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
}



