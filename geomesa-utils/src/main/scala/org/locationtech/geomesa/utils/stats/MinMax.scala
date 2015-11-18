/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature


case class MinMax[T <: Comparable[T]](attribute: String) extends Stat {

  var min: T = _
  var max: T = _

  override def observe(sf: SimpleFeature): Unit = {

    val sfval = sf.getAttribute(attribute)

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

  private def updateMin(sfval : T): Unit = {
    if (min == null) {
      min = sfval
    } else {
      if (min.compareTo(sfval) > 0) {
        min = sfval
      }
    }
  }

  private def updateMax(sfval : T): Unit = {
    if (max == null) {
      max = sfval
    } else {
      if (max.compareTo(sfval) < 0) {
        max = sfval
      }
    }
  }

  override def toJson(): String = s"$attribute: { min: $min, max: $max }"
}