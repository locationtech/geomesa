/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureOrdering, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.sort.{SortBy, SortOrder}

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
        case SortBy.NATURAL_ORDER => SimpleFeatureOrdering.fid
        case SortBy.REVERSE_ORDER => SimpleFeatureOrdering.fid.reverse
        case sb =>
          val prop = sb.getPropertyName.getPropertyName
          val idx = sft.indexOf(prop)
          require(idx != -1, s"Trying to sort on unavailable property '$prop' in feature type " +
              s"'${SimpleFeatureTypes.encodeType(sft)}'")
          val ord = SimpleFeatureOrdering(idx)
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

  override def hasNext: Boolean = sorted.hasNext

  override def next(): SimpleFeature = sorted.next()

  override def close(): Unit = features.close()
}

