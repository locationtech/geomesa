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
  * The IteratorStackCount keeps track of the number of times Accumulo sets up an iterator stack
  * as a result of a query.
  *
  * @param sft simple feature type
  */
class IteratorStackCount private [stats] (val sft: SimpleFeatureType) extends Stat {

  private [stats] var counter: Long = 1

  override type S = IteratorStackCount

  def count: Long = counter

  override def observe(sf: SimpleFeature): Unit = {}

  override def unobserve(sf: SimpleFeature): Unit = {}

  override def +(other: IteratorStackCount): IteratorStackCount = {
    val plus = new IteratorStackCount(sft)
    plus.counter += this.counter
    plus.counter += other.counter
    plus
  }

  override def +=(other: IteratorStackCount): Unit = counter += other.counter

  override def toJsonObject: Map[String, Long] = Map("count" -> counter)

  override def isEmpty: Boolean = false

  override def clear(): Unit = counter = 1L

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: IteratorStackCount => counter == that.counter
    case _ => false
  }
}
