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
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.impl.MinMax
import org.locationtech.geomesa.index.stats.impl.MinMax.MinMaxDefaults
import org.locationtech.jts.geom.Point

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
    if (!exact || filter == Filter.INCLUDE) {
      val i = sft.indexOf(attribute)
      if (sft.nonSpatialBounds().contains(i)) {
        val binding = sft.getDescriptor(i).getType.getBinding
        val minMax = new MinMax[T](sft, sft.getDescriptor(i).getLocalName)(MinMaxDefaults(binding))

        var min: String = null
        var max: String = null
        ds.storage(sft.getTypeName).metadata.getFiles(filter).foreach { file =>
          file.attributeBounds.filter(_.attribute == i).foreach { bounds =>
            if (max == null || max < bounds.upper) {
              max = bounds.upper
            }
            if (min == null || min > bounds.lower) {
              min = bounds.lower
            }
          }
        }
        if (min != null) {
          val alias = AttributeIndexKey.alias(binding)
          val sf = new ScalaSimpleFeature(sft, "")
          Seq(min, max).foreach { value =>
            sf.setAttribute(i, AttributeIndexKey.decode(alias, value))
            minMax.observe(sf)
          }
        }
        Some(minMax)
      } else if (sft.spatialBounds().contains(i) && sft.getDescriptor(i).getType.getBinding == classOf[Point]) {
        val minMax = new MinMax[T](sft, sft.getDescriptor(i).getLocalName)(MinMaxDefaults(classOf[Point]))

        var xmin, ymin, xmax, ymax: java.lang.Double = null
        ds.storage(sft.getTypeName).metadata.getFiles(filter).foreach { file =>
          file.spatialBounds.filter(_.attribute == i).foreach { bounds =>
            xmin = if (xmin == null) { bounds.xmin } else { math.min(xmin, bounds.xmin) }
            xmax = if (xmax == null) { bounds.xmax } else { math.max(xmax, bounds.xmax) }
            ymin = if (ymin == null) { bounds.ymin } else { math.min(ymin, bounds.ymin) }
            ymax = if (ymax == null) { bounds.ymax } else { math.max(ymax, bounds.ymax) }
          }
        }
        if (xmin != null) {
          val sf = new ScalaSimpleFeature(sft, "")
          Seq(xmin -> ymin, xmax -> ymax).foreach { case (x, y) =>
            sf.setAttribute(i, s"POINT ($x $y)")
            minMax.observe(sf)
          }
        }
        Some(minMax)
      } else {
        super.getMinMax(sft, attribute, filter, exact)
      }
    } else {
      super.getMinMax(sft, attribute, filter, exact)
    }
  }
}
