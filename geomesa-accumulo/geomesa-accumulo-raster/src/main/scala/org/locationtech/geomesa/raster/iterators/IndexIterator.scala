/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.iterators

import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}

/**
 * This is an Index Only Iterator, to be used in situations where the data records are
 * not useful enough to pay the penalty of decoding when using the
 * SpatioTemporalIntersectingIterator.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 */
class IndexIterator
    extends GeomesaFilteringIterator
    with HasFeatureType
    with SetTopIndexUnique
    with SetTopIndexFilterUnique
    with SetTopIndexTransformUnique
    with SetTopIndexFilterTransformUnique {

  var setTopOptimized: (Key) => Unit = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)

    // pick the execution path once based on the filters and transforms we need to apply
    // see org.locationtech.geomesa.core.iterators.IteratorFunctions
    setTopOptimized = (stFilter, transform, checkUniqueId) match {
      case (null, null, null) => setTopIndexInclude
      case (null, null, _)    => setTopIndexUnique
      case (_, null, null)    => setTopIndexFilter
      case (_, null, _)       => setTopIndexFilterUnique
      case (null, _, null)    => setTopIndexTransform
      case (null, _, _)       => setTopIndexTransformUnique
      case (_, _, null)       => setTopIndexFilterTransform
      case (_, _, _)          => setTopIndexFilterTransformUnique
    }
  }

  override def setTopConditionally(): Unit = setTopOptimized(source.getTopKey)
}
