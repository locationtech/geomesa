/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.iterators

import java.util.{Map => JMap}

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.raster.index.RasterEntry

class RasterFilteringIterator
  extends GeomesaFilteringIterator
  with HasFeatureType
  with SetTopUnique
  with SetTopFilter
  with HasFilter {

  var setTopOptimized: (Key) => Unit = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)
    logger.debug(s"In RFI with $filter")

    setTopOptimized = if (filter == null) setTopInclude else setTopFilter

  }

  override def setTopFilter(key: Key): Unit = {
    val value = source.getTopValue
    val sf = RasterEntry.decodeIndexCQMetadataToSf(key.getColumnQualifierData.toArray)
    if (filter.evaluate(sf)) {
      topKey = key
      topValue = value
    }
  }

  override def setTopConditionally(): Unit = setTopOptimized(source.getTopKey)

}

object RasterFilteringIterator {
  val name: String  = "raster-filtering-iterator"
  val priority: Int = 90
}
