/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.locationtech.geomesa.utils.collection.TieredOrdering
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.sort.{SortBy, SortOrder}

import scala.math.Ordering

/**
  * Ordering for simple features. Assumes that any attributes implement `Comparable`
  */
object SimpleFeatureOrdering {

  private val cached = Array.tabulate(16)(new AttributeOrdering(_))

  def apply(i: Int): Ordering[SimpleFeature] =
    if (i < cached.length) { cached(i) } else { new AttributeOrdering(i) }

  def apply(sft: SimpleFeatureType, sortBy: String): Ordering[SimpleFeature] =
    if (sortBy == null || sortBy.equalsIgnoreCase("id")) { fid } else { apply(sft.indexOf(sortBy)) }

  def apply(sft: SimpleFeatureType, sortBy: SortBy): Ordering[SimpleFeature] = {
    val property = sortBy.getPropertyName
    val ordering = if (property == null) { fid } else {
      val i = sft.indexOf(property.getPropertyName)
      if (i == -1) {
        new PropertyOrdering(property)
      } else {
        apply(i)
      }
    }
    if (sortBy.getSortOrder == SortOrder.ASCENDING) {
      ordering
    } else {
      ordering.reverse
    }
  }

  def apply(sft: SimpleFeatureType, sortBy: Array[SortBy]): Ordering[SimpleFeature] = {
    if (sortBy.length == 1) {
      apply(sft, sortBy.head)
    } else {
      TieredOrdering(sortBy.map(apply(sft, _)))
    }
  }

  def fid: Ordering[SimpleFeature] = Fid

  private object Fid extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int = x.getID.compareTo(y.getID)
  }

  private class AttributeOrdering(i: Int) extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int =
      nullCompare(x.getAttribute(i).asInstanceOf[Comparable[Any]], y.getAttribute(i))
  }

  private class PropertyOrdering(property: PropertyName) extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int =
      nullCompare(property.evaluate(x).asInstanceOf[Comparable[Any]], property.evaluate(y))
  }

  /**
    * Compares two values, nulls are ordered first
    *
    * @param x left value
    * @param y right value
    * @return
    */
  def nullCompare(x: Comparable[Any], y: Any): Int = {
    if (x == null) {
      if (y == null) { 0 } else { -1 }
    } else if (y == null) {
      1
    } else {
      x.compareTo(y)
    }
  }
}

