/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable

case class EnumeratedHistogram[T](attribute: String) extends Stat {

  val map: scala.collection.mutable.HashMap[T, Long] = mutable.HashMap[T, Long]()

  override def observe(sf: SimpleFeature): Unit = {
    val sfval = sf.getAttribute(attribute)

    if (sfval != null) {
      sfval match {
        case tval: T =>
          updateMap(tval, 1)
      }
    }
  }

  override def toJson(): String = {
    s"[${map.map{ case (key: T, count: Long) => s"$key:$count" }.mkString(",")}]"
  }

  override def add(other: Stat): Stat = {
    other match {
      case eh: EnumeratedHistogram[T] =>
        combine(eh)
        this
    }
  }

  private def updateMap(key: T, increment: Long) = {
    map.get(key) match {
      case Some(long) => map.update(key, long + increment)
      case None       => map.update(key, increment)
    }
  }

  private def combine(eh: EnumeratedHistogram[T]): Unit =
    eh.map.foreach { case (key: T, count: Long) => updateMap(key, count) }
}
