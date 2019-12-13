/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.iterators

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}

class IndexedSpatioTemporalFilter
    extends GeomesaFilteringIterator
    with HasFeatureType
    with HasIndexValueDecoder
    with HasSpatioTemporalFilter
    with LazyLogging {

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)
    this.source = source.deepCopy(env)
  }

  override def setTopConditionally() = {
    val sourceValue = source.getTopValue
    val meetsFilter = stFilter == null || {
      val sf = indexEncoder.deserialize(sourceValue.get)
      stFilter.evaluate(sf)
    }
    if (meetsFilter) {
      topKey = source.getTopKey
      topValue = sourceValue
    }
  }
}
