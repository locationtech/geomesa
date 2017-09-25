/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import org.locationtech.geomesa.utils.conversions.ScalaImplicits.TieredOrdering
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.sort.{SortBy, SortOrder}

import scala.math.Ordering

object SimpleFeatureOrdering {

  def apply(sft: SimpleFeatureType, sortBy: String, reverse: Boolean): Ordering[SimpleFeature] = {
    val ordering = if (sortBy == null || sortBy.equalsIgnoreCase("id")) {
      IdOrdering
    } else {
      new AttributeOrdering(sft.indexOf(sortBy))
    }
    if (reverse) { ordering.reverse } else { ordering }
  }

  def apply(sft: SimpleFeatureType, sortBy: Array[SortBy]): Option[Ordering[SimpleFeature]] = {
    val sort = Option(sortBy).getOrElse(Array.empty).map { sort =>
      val property = sort.getPropertyName
      val ordering = if (property == null) { IdOrdering } else {
        val i = sft.indexOf(property.getPropertyName)
        if (i == -1) {
          new PropertyOrdering(property)
        } else {
          new AttributeOrdering(i)
        }
      }
      if (sort.getSortOrder == SortOrder.ASCENDING) {
        ordering
      } else {
        ordering.reverse
      }
    }
    sort.length match {
      case 0 => None
      case 1 => Some(sort(0))
      case _ => Some(new TieredOrdering(sort))
    }
  }

  object IdOrdering extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int = x.getID.compareTo(y.getID)
  }

  class AttributeOrdering (i: Int) extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int =
      nullCompare(x.getAttribute(i).asInstanceOf[Comparable[Any]], y.getAttribute(i).asInstanceOf[Comparable[Any]])
  }

  class PropertyOrdering (property: PropertyName) extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int =
      nullCompare(property.evaluate(x).asInstanceOf[Comparable[Any]], property.evaluate(y).asInstanceOf[Comparable[Any]])
  }

  private def nullCompare(x: Comparable[Any], y: Comparable[Any]): Int = {
    if (x == null) {
      if (y == null) {
        0
      } else {
        -1
      }
    } else if (y == null) {
      1
    } else {
      x.compareTo(y)
    }
  }
}
