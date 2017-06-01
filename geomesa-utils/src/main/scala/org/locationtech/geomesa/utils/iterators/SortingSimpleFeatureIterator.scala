/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.sort.{SortBy, SortOrder}

import scala.reflect.ClassTag

/**
  * In memory sorting of simple features
  *
  * @param features unsorted feature iterator
  * @param sortBy attributes to sort by
  */
class SortingSimpleFeatureIterator(features: CloseableIterator[SimpleFeature], sortBy: Array[SortBy])
    extends CloseableIterator[SimpleFeature] {

  private lazy val sorted: CloseableIterator[SimpleFeature] = {
    if (!features.hasNext) { features } else {
      val first = features.next()
      val sft = first.getFeatureType

      val sortOrdering = sortBy.map {
        case SortBy.NATURAL_ORDER => Ordering.by[SimpleFeature, String](_.getID)
        case SortBy.REVERSE_ORDER => Ordering.by[SimpleFeature, String](_.getID).reverse
        case sb =>
          val prop = sb.getPropertyName.getPropertyName
          val idx = sft.indexOf(prop)
          require(idx != -1, s"Trying to sort on unavailable property '$prop' in feature type " +
              s"'${SimpleFeatureTypes.encodeType(sft)}'")
          val ord  = attributeToComparable(idx)
          if (sb.getSortOrder == SortOrder.DESCENDING) ord.reverse else ord
      }
      val comp: (SimpleFeature, SimpleFeature) => Boolean =
        if (sortOrdering.length == 1) {
          // optimized case for one ordering
          val ret = sortOrdering.head
          (l, r) => ret.compare(l, r) < 0
        } else {
          (l, r) => sortOrdering.map(_.compare(l, r)).find(_ != 0).getOrElse(0) < 0
        }
      // use ListBuffer for constant append time and size
      val buf = scala.collection.mutable.ListBuffer(first)
      while (features.hasNext) {
        buf.append(features.next())
      }
      features.close()
      CloseableIterator(buf.sortWith(comp).iterator)
    }
  }

  def attributeToComparable[T <: Comparable[T]](i: Int)(implicit ct: ClassTag[T]): Ordering[SimpleFeature] =
    Ordering.by[SimpleFeature, T](_.getAttribute(i).asInstanceOf[T])(new Ordering[T] {
      val evo = implicitly[Ordering[T]]

      override def compare(x: T, y: T): Int = {
        if (x == null) {
          if (y == null) { 0 } else { -1 }
        } else if (y == null) {
          1
        } else {
          evo.compare(x, y)
        }
      }
    })

  override def hasNext: Boolean = sorted.hasNext

  override def next(): SimpleFeature = sorted.next()

  override def close(): Unit = features.close()
}

