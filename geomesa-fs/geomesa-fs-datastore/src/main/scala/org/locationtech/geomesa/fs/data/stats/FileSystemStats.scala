/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data.stats

import org.apache.iceberg.types.Conversions
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.core.schema.ColumnName
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
    if (!exact || filter == Filter.INCLUDE) {
      Some(ds.storage(sft.getTypeName).metadata.files().forFilter(filter).scan().map(_.recordCount()).sum)
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
      val minMax = Stat(sft, Stat.MinMax(attribute)).asInstanceOf[MinMax[T]]
      val storage = ds.storage(sft.getTypeName)
      val sf = new ScalaSimpleFeature(sft, "")
      val i = sft.indexOf(attribute)
      val field = storage.schema.schema.findField(ColumnName.encode(attribute))
      val fieldId = field.fieldId()
      val fieldType = field.`type`()
      ds.storage(sft.getTypeName).metadata.files().includeFileStats().forFilter(filter).scan().foreach { f =>
        Seq(f.lowerBounds().get(fieldId), f.upperBounds().get(fieldId)).foreach { buffer =>
          if (buffer != null) {
            sf.setAttribute(i, Conversions.fromByteBuffer[AnyRef](fieldType, buffer))
            minMax.observe(sf)
          }
        }
      }
      Some(minMax)
    } else {
      super.getMinMax(sft, attribute, filter, exact)
    }
  }
}
