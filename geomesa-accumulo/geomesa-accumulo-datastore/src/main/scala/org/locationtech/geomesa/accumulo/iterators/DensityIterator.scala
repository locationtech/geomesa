/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.index.iterators.DensityScan.DensityResult
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
 * Density iterator - only works on kryo-encoded features
 */
class DensityIterator extends BaseAggregatingIterator[DensityResult] with DensityScan

object DensityIterator extends LazyLogging {

  val DEFAULT_PRIORITY = 25

  /**
   * Creates an iterator config for the kryo density iterator
   */
  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val is = new IteratorSetting(priority, "density-iter", classOf[DensityIterator])
    DensityScan.configure(sft, index, filter, hints).foreach { case (k, v) => is.addOption(k, v) }
    is
  }

  /**
   * Adapts the iterator to create simple features.
   * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
   */
  def kvsToFeatures(): Entry[Key, Value] => SimpleFeature = {
    val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "")
    sf.setAttribute(0, GeometryUtils.zeroPoint)
    e: Entry[Key, Value] => {
      // Return value in user data so it's preserved when passed through a RetypingFeatureCollection
      sf.getUserData.put(DensityScan.DensityValueKey, e.getValue.get())
      sf
    }
  }

}
