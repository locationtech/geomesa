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
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.ColumnBounds
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.impl.MinMax
import org.locationtech.geomesa.index.stats.impl.MinMax.MinMaxDefaults

/**
 * Optimized stats using per-file bounds for non-exact cases
 *
 * @param ds datastore
 */
class FileSystemStats(ds: FileSystemDataStore) extends UnoptimizedRunnableStats(ds) {

  import org.locationtech.geomesa.fs.storage.core.RichSimpleFeatureType

  override def getCount(
      sft: SimpleFeatureType,
      filter: Filter,
      exact: Boolean,
      queryHints: Hints): Option[Long] = {
    if (!exact || filter == Filter.INCLUDE) {
      Some(ds.storage(sft.getTypeName).metadata.getFiles(filter).map(_.count).sum)
    } else {
      super.getCount(sft, filter, exact, queryHints)
    }
  }

  override def getMinMax[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[MinMax[T]] = {
    val i = sft.indexOf(attribute)
    if ((!exact || filter == Filter.INCLUDE) && sft.columnBounds().contains(i)) {
      var min: String = null
      var max: String = null
      ds.storage(sft.getTypeName).metadata.getFiles(filter).foreach { file =>
        file.bounds.filter(_.attribute == i).foreach { bounds =>
          if (max == null || max < bounds.upper) {
            max = bounds.upper
          }
          if (min == null || min > bounds.lower) {
            min = bounds.lower
          }
        }
      }
      val minMax = new MinMax[T](sft, sft.getDescriptor(i).getLocalName)(MinMaxDefaults(sft.getDescriptor(i).getType.getBinding))
      if (min != null) {
        val sf = new ScalaSimpleFeature(sft, "")
        ColumnBounds(i, min, max).decode(sft).productIterator.foreach { value =>
          sf.setAttribute(i, value.asInstanceOf[AnyRef])
          minMax.observe(sf)
        }
      }
      Some(minMax)
    } else {
      super.getMinMax(sft, attribute, filter, exact)
    }
  }
}
