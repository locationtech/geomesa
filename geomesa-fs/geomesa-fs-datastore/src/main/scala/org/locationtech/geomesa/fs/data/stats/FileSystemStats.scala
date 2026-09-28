/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data.stats

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.Stat
import org.locationtech.geomesa.index.stats.impl.MinMax

/**
 * Optimized stats using per-file bounds for non-exact cases
 *
 * @param ds datastore
 */
class FileSystemStats(ds: FileSystemDataStore) extends UnoptimizedRunnableStats(ds) {

  override def getCount(
      sft: SimpleFeatureType,
      filter: Filter,
      exact: Boolean,
      queryHints: Hints): Option[Long] = {
    Some(ds.storage(sft.getTypeName).getCount(filter, 1))
  }

  override def getMinMax[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[MinMax[T]] = {
    val (min, max) = ds.storage(sft.getTypeName).getBounds[T](attribute, filter, 1)
    val minMax = Stat(sft, Stat.MinMax(attribute)).asInstanceOf[MinMax[T]]
    val sf = new ScalaSimpleFeature(sft, "")
    Seq(min, max).foreach { value =>
      sf.setAttribute(attribute, value.asInstanceOf[AnyRef])
      minMax.observe(sf)
    }
    Some(minMax)
  }
}
