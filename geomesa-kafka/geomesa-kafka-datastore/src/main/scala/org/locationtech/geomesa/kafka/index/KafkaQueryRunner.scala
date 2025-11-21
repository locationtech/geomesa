/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import io.micrometer.core.instrument.Tags
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStore

class KafkaQueryRunner(ds: KafkaDataStore, caches: String => KafkaFeatureCache)
    extends LocalQueryRunner(Option(ds.config.authProvider)) {

  override protected val interceptors: QueryInterceptorFactory = ds.interceptors

  override protected def tags(typeName: String): Tags = ds.tags(typeName)

  override protected def features(sft: SimpleFeatureType, filter: Option[Filter]): Iterator[SimpleFeature] =
    caches(sft.getTypeName).query(filter.getOrElse(Filter.INCLUDE))
}
