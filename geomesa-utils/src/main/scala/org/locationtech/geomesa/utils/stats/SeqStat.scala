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
 * If the stats parser receives a string with multiple stats, a SeqStat will be used.
 *
 * @param stats a Sequence of individual Stat objects
 */
class SeqStat(val stats: Seq[Stat]) extends Stat {
  override def observe(sf: SimpleFeature): Unit = stats.foreach(_.observe(sf))

  override def add(other: Stat): Stat = {
    stats.zip(other.asInstanceOf[SeqStat].stats).foreach {
      case (stat1, stat2) => stat1.add(stat2)
    }

    this
  }

  override def toJson(): String = "[ " + stats.map(_.toJson()).mkString(", ") + " ]"

  override def clear(): Unit = stats.foreach(_.clear())

  override def equals(obj: Any): Boolean = {
    obj match {
      case seqStat: SeqStat =>
        stats == seqStat.stats
      case _ => false
    }
  }
}
