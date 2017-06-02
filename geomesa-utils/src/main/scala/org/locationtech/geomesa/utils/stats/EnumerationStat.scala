/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

/**
 * An enumeration is merely a HashMap mapping values to number of occurrences
 *
 * @param attribute attribute index for the attribute the histogram is being made for
 * @tparam T some type T (which is restricted by the stat parser upstream of Histogram instantiation)
 */
class EnumerationStat[T](val attribute: Int)(implicit ct: ClassTag[T]) extends Stat {

  override type S = EnumerationStat[T]

  private lazy val stringify = Stat.stringifier(ct.runtimeClass)

  private [stats] val enumeration = scala.collection.mutable.HashMap.empty[T, Long].withDefaultValue(0)

  def size: Int = enumeration.size
  def values: Iterable[T] = enumeration.keys
  def frequency(value: T): Long = enumeration(value)
  def frequencies: Iterable[(T, Long)] = enumeration

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute).asInstanceOf[T]
    if (value != null) {
      enumeration(value) += 1
    }
  }

  override def unobserve(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute).asInstanceOf[T]
    if (value != null) {
      val current = enumeration(value)
      if (current == 1) {
        enumeration.remove(value)
      } else {
        enumeration(value) = current - 1
      }
    }
  }

  override def +(other: EnumerationStat[T]): EnumerationStat[T] = {
    val plus = new EnumerationStat[T](attribute)
    plus += this
    plus += other
    plus
  }

  override def +=(other: EnumerationStat[T]): Unit =
    other.enumeration.foreach { case (key, count) => enumeration(key) += count }

  override def toJsonObject = if (enumeration.isEmpty) Map.empty else ListMap(enumeration.toSeq.sortBy(_.toString):_*)

  override def isEmpty: Boolean = enumeration.isEmpty

  override def clear(): Unit = enumeration.clear()

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: EnumerationStat[_] => attribute == that.attribute && enumeration == that.enumeration
    case _ => false
  }
}
