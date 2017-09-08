/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.opengis.feature.simple.SimpleFeature

import scala.math.Ordering

/**
  * Ordering for simple features. Assumes that any attributes implement `Comparable`
  */
object SimpleFeatureOrdering {

  private val cached = 16
  private val nullLastOrderings = Array.tabulate(cached)(new NullLastOrdering(_))
  private val nullFirstOrderings = Array.tabulate(cached)(new NullFirstOrdering(_))

  def apply(i: Int): Ordering[SimpleFeature] =
    if (i < cached) { nullFirstOrderings(i) } else { new NullFirstOrdering(i) }

  def fid: Ordering[SimpleFeature] = Fid

  def nullsFirst(i: Int): Ordering[SimpleFeature] = apply(i)

  def nullsLast(i: Int): Ordering[SimpleFeature] =
    if (i < cached) { nullLastOrderings(i) } else { new NullLastOrdering(i) }

  private object Fid extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int = x.getID.compareTo(y.getID)
  }

  private class NullLastOrdering(i: Int) extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int = {
      val left = x.getAttribute(i).asInstanceOf[Comparable[Any]]
      val right = y.getAttribute(i).asInstanceOf[Comparable[Any]]
      if (left == null) {
        if (right == null) { 0 } else { 1 }
      } else if (right == null) {
        -1
      } else {
        left.compareTo(right)
      }
    }
  }

  private class NullFirstOrdering(i: Int) extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int = {
      val left = x.getAttribute(i).asInstanceOf[Comparable[Any]]
      val right = y.getAttribute(i).asInstanceOf[Comparable[Any]]
      if (left == null) {
        if (right == null) { 0 } else { -1 }
      } else if (right == null) {
        1
      } else {
        left.compareTo(right)
      }
    }
  }
}

