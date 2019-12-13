/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.iterators

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}

/**
 * This iterator returns as its nextKey and nextValue responses the key and value
 * from the DATA iterator, not from the INDEX iterator.  The assumption is that
 * the data rows are what we care about; that we do not care about the index
 * rows that merely helped us find the data rows quickly.
 *
 * The other trick to remember about iterators is that they essentially pre-fetch
 * data.  "hasNext" really means, "was there a next record that you already found".
 */
class SpatioTemporalIntersectingIterator
    extends GeomesaFilteringIterator
    with HasFeatureType
    with SetTopUnique
    with SetTopFilterUnique
    with SetTopTransformUnique
    with SetTopFilterTransformUnique {

  var setTopOptimized: (Key) => Unit = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)

    // pick the execution path once based on the filters and transforms we need to apply
    // see org.locationtech.geomesa.core.iterators.IteratorFunctions
    setTopOptimized = (filter, transform, checkUniqueId) match {
      case (null, null, null) => setTopInclude
      case (null, null, _)    => setTopUnique
      case (_, null, null)    => setTopFilter
      case (_, null, _)       => setTopFilterUnique
      case (null, _, null)    => setTopTransform
      case (null, _, _)       => setTopTransformUnique
      case (_, _, null)       => setTopFilterTransform
      case (_, _, _)          => setTopFilterTransformUnique
    }
  }

  override def setTopConditionally(): Unit = setTopOptimized(source.getTopKey)
}
