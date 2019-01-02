/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Counts features
  *
  * @param sft simple feature type
  */
class CountStat private [stats] (val sft: SimpleFeatureType) extends Stat {

  override type S = CountStat

  private [stats] var counter: Long = 0L

  def count: Long = counter

  override def observe(sf: SimpleFeature): Unit = counter += 1

  override def unobserve(sf: SimpleFeature): Unit = counter -= 1

  override def +(other: CountStat): CountStat = {
    val plus = new CountStat(sft)
    plus.counter = this.counter + other.counter
    plus
  }

  override def +=(other: CountStat): Unit = counter += other.counter

  override def toJsonObject: Map[String, Long] = Map("count" -> counter)

  override def isEmpty: Boolean = counter == 0

  override def clear(): Unit = counter = 0

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: CountStat => counter == that.counter
    case _ => false
  }
}
