/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning.guard

import org.geotools.api.data.{DataStore, Query}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.planning.QueryInterceptor

/**
 * Query guard that enforces the default full table scan checks
 */
class DefaultQueryGuard extends QueryInterceptor {

  override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {}

  override def guard(strategy: QueryStrategy): Option[IllegalArgumentException] =
    strategy.index.checkQueryBlock(strategy, blockByDefault = false)

  override def rewrite(query: Query): Unit = {}

  override def close(): Unit = {}
}
