/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
 * Note: we don't need to weight hits based on split geometries, as otherwise polygons render
 * much lighter than points
 */
@deprecated
class Z2DensityIterator extends KryoLazyDensityIterator

object Z2DensityIterator {

  val TableSharingKey = "ts"

  /**
   * Creates an iterator config for the z2 density iterator
   */
  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndex,
                filter: Option[Filter],
                hints: Hints,
                priority: Int = KryoLazyDensityIterator.DEFAULT_PRIORITY): IteratorSetting = {
    val is = KryoLazyDensityIterator.configure(sft, index, filter, hints, deduplicate = false, priority)
    is.setIteratorClass(classOf[Z2DensityIterator].getName)
    is.addOption(TableSharingKey, sft.isTableSharing.toString)
    is
  }
}