/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import com.github.benmanes.caffeine.cache.LoadingCache
import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.kafka.data.{KafkaCacheLoader, KafkaDataStore}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class KafkaQueryRunner(ds: KafkaDataStore, caches: LoadingCache[String, KafkaCacheLoader])
    extends LocalQueryRunner(ds.stats, Option(ds.config.authProvider)) {

  override protected def name: String = "Kafka"

  override protected val interceptors: QueryInterceptorFactory = ds.interceptors

  override protected def features(sft: SimpleFeatureType, filter: Option[Filter]): CloseableIterator[SimpleFeature] =
    CloseableIterator(caches.get(sft.getTypeName).cache.query(filter.getOrElse(Filter.INCLUDE)))
}
