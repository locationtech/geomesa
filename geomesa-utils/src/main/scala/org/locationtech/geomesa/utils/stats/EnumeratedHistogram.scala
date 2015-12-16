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
import scala.util.parsing.json.JSONObject

/**
 * An EnumeratedHistogram is merely a HashMap mapping values to number of occurences
 * .
 * @param attrIndex attribute index for the attribute the histogram is being made for
 * @param attrType class type as a string for serialization purposes
 * @param frequencyMap HashMap which backs the EnumeratedHistogram
 * @tparam T some type T (which is restricted by the stat parser upstream of EnumeratedHistogram instantiation)
 */
case class EnumeratedHistogram[T](attrIndex: Int,
                                  attrType: String,
                                  frequencyMap: mutable.Map[T, Long] = new mutable.HashMap[T, Long]().withDefaultValue(0)) extends Stat {
  override def observe(sf: SimpleFeature): Unit = {
    val sfval = sf.getAttribute(attrIndex)
    if (sfval != null) {
      sfval match {
        case tval: T =>
          frequencyMap(tval) += 1
      }
    }
  }

  override def add(other: Stat): Stat = {
    other match {
      case eh: EnumeratedHistogram[T] =>
        combine(eh)
        this
    }
  }

  private def combine(other: EnumeratedHistogram[T]): Unit =
    other.frequencyMap.foreach { case (key: T, count: Long) => frequencyMap(key) += count }

  override def toJson(): String = {
    val jsonMap = frequencyMap.toMap.map { case (k, v) => k.toString -> v }
    new JSONObject(jsonMap).toString()
  }

  override def isEmpty(): Boolean = ???

  override def clear(): Unit = ???
}


