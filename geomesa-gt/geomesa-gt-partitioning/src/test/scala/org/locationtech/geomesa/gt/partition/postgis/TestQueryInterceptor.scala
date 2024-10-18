/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.geotools.api.data.{DataStore, Query}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.index.planning.QueryInterceptor

class TestQueryInterceptor extends QueryInterceptor {

  var sft: SimpleFeatureType = _

  override def init(ds: DataStore, sft: SimpleFeatureType): Unit = this.sft = sft

  override def rewrite(query: Query): Unit = query.setFilter(Filter.INCLUDE)

  override def close(): Unit = {}
}
