/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

import scala.reflect.ClassTag

/**
 * A histogram is merely a HashMap mapping values to number of occurrences
 *
 * @param attribute attribute index for the attribute the histogram is being made for
 * @tparam T some type T (which is restricted by the stat parser upstream of Histogram instantiation)
 */
class Histogram[T](val attribute: Int)(implicit ct: ClassTag[T]) extends Stat {

  override type S = Histogram[T]

  private lazy val stringify = Stat.stringifier(ct.runtimeClass)

  private [stats] val histogram = scala.collection.mutable.HashMap.empty[T, Long].withDefaultValue(0)

  def size: Int = histogram.size
  def values: Iterable[T] = histogram.keys
  def frequency(value: T): Long = histogram(value)

  override def observe(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute).asInstanceOf[T]
    if (value != null) {
      histogram(value) += 1
    }
  }

  override def unobserve(sf: SimpleFeature): Unit = {
    val value = sf.getAttribute(attribute).asInstanceOf[T]
    if (value != null) {
      val current = histogram(value)
      if (current == 1) {
        histogram.remove(value)
      } else {
        histogram(value) = current - 1
      }
    }
  }

  override def +(other: Histogram[T]): Histogram[T] = {
    val plus = new Histogram[T](attribute)
    plus += this
    plus += other
    plus
  }

  override def +=(other: Histogram[T]): Unit =
    other.histogram.foreach { case (key, count) => histogram(key) += count }

  override def toJson: String = {
    if (histogram.isEmpty) { "{ }" } else {
      histogram.toSeq.sortBy(_.toString).map { case (k, v) => s""""${stringify(k)}" : $v""" }.mkString("{ ", ", ", " }")
    }
  }

  override def isEmpty: Boolean = histogram.isEmpty

  override def clear(): Unit = histogram.clear()

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: Histogram[_] => attribute == that.attribute && histogram == that.histogram
    case _ => false
  }
}
