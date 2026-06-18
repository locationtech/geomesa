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
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.impl.MinMax

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
      Some(ds.storage(sft.getTypeName).metadata.getFiles(filter).map(_.recordCount()).sum)
    } else {
      super.getCount(sft, filter, exact, queryHints)
    }
  }

  override def getMinMax[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter,
      exact: Boolean): Option[MinMax[T]] = {
    // TODO: Extract min/max from DataFile.lowerBounds()/upperBounds()
    // This requires mapping attribute indices to Iceberg field IDs and decoding ByteBuffers
    // For now, falling back to the base implementation which will scan features
    super.getMinMax(sft, attribute, filter, exact)
  }
}
