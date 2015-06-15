/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}

/**
 * Iterator for the record table. Applies transforms and ECQL filters.
 */
class RecordTableIterator
    extends GeomesaFilteringIterator
    with HasFeatureType
    with SetTopInclude
    with SetTopFilter
    with SetTopTransform
    with SetTopFilterTransform {

  var setTopOptimized: (Key) => Unit = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)

    // pick the execution path once based on the filters and transforms we need to apply
    // see org.locationtech.geomesa.core.iterators.IteratorFunctions
    setTopOptimized = (filter, transform) match {
      case (null, null) => setTopInclude
      case (_, null)    => setTopFilter
      case (null, _)    => setTopTransform
      case (_, _)       => setTopFilterTransform
    }
  }

  override def setTopConditionally(): Unit = setTopOptimized(source.getTopKey)
}


