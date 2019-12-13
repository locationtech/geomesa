/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureOrdering
import org.opengis.feature.simple.SimpleFeature

/**
  * In memory sorting of simple features
  *
  * @param features unsorted feature iterator
  * @param sortBy attributes to sort by, in the form: (name, reverse).
  *               for sort by feature id (e.g. natural sort), use an empty string for name
  */
class SortingSimpleFeatureIterator(features: CloseableIterator[SimpleFeature], sortBy: Seq[(String, Boolean)])
    extends CloseableIterator[SimpleFeature] {

  private lazy val sorted: CloseableIterator[SimpleFeature] = {
    if (!features.hasNext) { features } else {
      val first = features.next()
      val sft = first.getFeatureType
      val ordering = SimpleFeatureOrdering(sft, sortBy)
      // use ListBuffer for constant append time and size
      val buf = scala.collection.mutable.ListBuffer(first)
      while (features.hasNext) {
        buf.append(features.next())
      }
      features.close()
      CloseableIterator(buf.sorted(ordering).iterator)
    }
  }

  override def hasNext: Boolean = sorted.hasNext

  override def next(): SimpleFeature = sorted.next()

  override def close(): Unit = features.close()
}

