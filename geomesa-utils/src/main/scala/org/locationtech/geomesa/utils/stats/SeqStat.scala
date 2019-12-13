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
  * If the stats parser receives a string with multiple stats, a SeqStat will be used.
  *
  * @param sft simple feature type
  * @param stats a Sequence of individual Stat objects
  */
class SeqStat(val sft: SimpleFeatureType, val stats: Seq[Stat]) extends Stat {

  override type S = SeqStat

  override def observe(sf: SimpleFeature): Unit = stats.foreach(_.observe(sf))

  override def unobserve(sf: SimpleFeature): Unit = stats.foreach(_.unobserve(sf))

  override def +(other: SeqStat): SeqStat = {
    val builder = Seq.newBuilder[Stat]
    builder.sizeHint(stats.length)
    val iter = other.stats.iterator
    stats.foreach { stat =>
      if (iter.hasNext) {
        builder += (stat + iter.next())
      }
    }
    new SeqStat(sft, builder.result())
  }


  override def +=(other: SeqStat): Unit = {
    val iter = other.stats.iterator
    stats.foreach { stat =>
      if (iter.hasNext) {
        stat += iter.next()
      }
    }
  }

  override def toJsonObject: Seq[Any] = stats.map(_.toJsonObject)

  override def isEmpty: Boolean = stats.forall(_.isEmpty)

  override def clear(): Unit = stats.foreach(_.clear())

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: SeqStat =>
      stats.length == that.stats.length && {
        val iter = that.stats.iterator
        stats.forall(stat => iter.hasNext && stat.isEquivalent(iter.next))
      }

    case _ => false
  }
}
