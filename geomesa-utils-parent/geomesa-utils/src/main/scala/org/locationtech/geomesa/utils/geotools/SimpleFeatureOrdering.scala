/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.locationtech.geomesa.utils.collection.TieredOrdering
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.sort.{SortBy, SortOrder}

import scala.math.Ordering

/**
  * Ordering for simple features
  */
object SimpleFeatureOrdering {

  /**
   * Sort on an attribute by name. `null`, `"id"` or an empty string can be used to indicate
   * 'natural' ordering by feature ID
   *
   * @param sft simple feature type
   * @param sortBy attribute to sort by
   * @return
   */
  def apply(sft: SimpleFeatureType, sortBy: String): Ordering[SimpleFeature] = apply(sft, sortBy, reverse = false)

  /**
   * Sort on an attribute by name. `null`, `"id"` or an empty string can be used to indicate
   * * 'natural' ordering by feature ID
   *
   * @param sft simple feature type
   * @param sortBy attribute to sort by
   * @param reverse reverse the sort (from ascending to descending)
   * @return
   */
  def apply(sft: SimpleFeatureType, sortBy: String, reverse: Boolean): Ordering[SimpleFeature] = {
    val sort = if (sortBy == null || sortBy.isEmpty || sortBy.equalsIgnoreCase("id")) { FidOrdering } else {
      val i = sft.indexOf(sortBy)
      if (i == -1) {
        throw new IllegalArgumentException(s"Trying to sort by an attribute that is not in the schema: $sortBy")
      }
      new SimpleFeatureOrdering(i, AttributeOrdering(sft.getDescriptor(i)))
    }
    if (reverse) { sort.reverse } else { sort }
  }

  /**
   * Sort by multiple attributes by name
   *
   * @param sft simple feature type
   * @param sortBy pairs of (attribute name, reverse ordering)
   * @return
   */
  def apply(sft: SimpleFeatureType, sortBy: Seq[(String, Boolean)]): Ordering[SimpleFeature] = {
    if (sortBy.lengthCompare(1) == 0) {
      apply(sft, sortBy.head._1, sortBy.head._2)
    } else {
      TieredOrdering(sortBy.map { case (field, reverse) => apply(sft, field, reverse) })
    }
  }

  /**
   * Sort on a geotools SortBy instance
   *
   * @param sft simple feature type
   * @param sortBy sort by
   * @return
   */
  def apply(sft: SimpleFeatureType, sortBy: SortBy): Ordering[SimpleFeature] = {
    val name = Option(sortBy.getPropertyName).map(_.getPropertyName).orNull
    apply(sft, name, sortBy.getSortOrder == SortOrder.DESCENDING)
  }

  /**
   * Sort on a geotools SortBy array
   *
   * @param sft simple feature type
   * @param sortBy sort by
   * @return
   */
  def apply(sft: SimpleFeatureType, sortBy: Array[SortBy]): Ordering[SimpleFeature] = {
    if (sortBy.length == 1) {
      apply(sft, sortBy.head)
    } else {
      TieredOrdering(sortBy.map(apply(sft, _)))
    }
  }

  def fid: Ordering[SimpleFeature] = FidOrdering

  object FidOrdering extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int = x.getID.compareTo(y.getID)
  }

  private class SimpleFeatureOrdering(i: Int, delegate: Ordering[AnyRef]) extends Ordering[SimpleFeature] {
    override def compare(x: SimpleFeature, y: SimpleFeature): Int =
      delegate.compare(x.getAttribute(i), y.getAttribute(i))
  }
}

