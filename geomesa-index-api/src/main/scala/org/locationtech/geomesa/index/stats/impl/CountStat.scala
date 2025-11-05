/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.stats.impl

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.index.stats.Stat

/**
  * Counts features
  *
  * @param sft simple feature type
  */
class CountStat(val sft: SimpleFeatureType, private [stats] var counter: Long = 0L) extends Stat {

  override type S = CountStat

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

  override def isEmpty: Boolean = counter == 0L

  override def clear(): Unit = counter = 0

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: CountStat => counter == that.counter
    case _ => false
  }
}
